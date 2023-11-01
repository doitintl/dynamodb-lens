import logging
from datetime import datetime, timedelta
from math import ceil
from dynamodb_lens.utils import json_dumps_iso, return_boto3_client


DESCRIPTION = '''This library analyzes a table's current state, configuration and usage in order to estimate performance capabilities.     
A DynamoDB Table's current partition count as well as table settings will determine its performance capabilities.       
Partition count is not exposed directly, but we can infer the number of partitions using several methods.

If Streams is not enabled, this program will estimate current number of partitions based on the maximum WCU/RCU values 
calculated from the data gathered.    

***This program is more accurate if DynamoDB Streams is enabled for the table.***   
Please view the README for more details.
'''


class TableAnalyzer:
    def __init__(
            self,
            table_name,
            lambda_client=return_boto3_client('lambda'),
            ddbs_client=return_boto3_client('dynamodbstreams'),
            ddb_client=return_boto3_client('dynamodb'),
            cw_client=return_boto3_client('cloudwatch'),
            verbose=False):
        self.table_name = table_name
        self.analysis = None
        self._lambda_client = lambda_client
        self._ddbs_client = ddbs_client
        self._ddb_client = ddb_client
        self._cw_client = cw_client
        self._verbose = verbose
        self._table_desc = None
        self._stream_desc = None
        self._table_arn = None
        self._stream_arn = None
        self._size_in_bytes = None
        self._size_in_mb = None
        self._size_in_gb = None
        self._item_count = None
        self._ec_multiplier = 2
        self._wcu_size_bytes = 1000
        self._rcu_size_bytes = 4000
        self._rcu_part_hard_limit = 3000
        self._wcu_part_hard_limit = 1000
        self._rcu_part_soft_limit = None
        self._wcu_part_soft_limit = None
        self._partitions = None
        self._billing_mode = None
        self._part_wcu = None
        self._part_rcu = None
        self._max_rcu = None
        self._max_read_throughput_bytes = None
        self._max_wcu = None
        self._max_write_throughput_bytes = None
        self._summary = None
        self._stream_enabled = None
        self._deletion_protection = None
        self._stream_open_shards = 0
        self._stream_closed_shards = 0
        self._stream_total_shards = 0
        self._metrics_data = {
            'ConsumedWCU': {
                'Timestamps': [],
                'Values': []
            },
            'ConsumedRCU': {
                'Timestamps': [],
                'Values': []
            },
            'ProvisionedWCU': {
                'Timestamps': [],
                'Values': []
            },
            'ProvisionedRCU': {
                'Timestamps': [],
                'Values': []
            }
        }
        self._estimations_dict = {}
        self._analyze_table()

    def __str__(self):
        return DESCRIPTION

    def _analyze_table(self):
        logging.info(f'Analyzing table: {self.table_name}')
        self._describe_table()
        self._is_stream_enabled()
        if self._stream_enabled:
            logging.info(f'Describing stream: {self._stream_arn}. This can take a while on high performance tables.')
            self._describe_stream()
        logging.info(f'Pulling Cloudwatch metrics for table: {self.table_name}')
        self._get_metric_data()
        logging.info(f'Crunching numbers...')
        self._generate_estimations_data()
        self._estimate_partitions()
        self._estimate_maximums()
        self._generate_summary()
        self._generate_analysis()

    def _is_stream_enabled(self):
        self._stream_enabled = self._table_desc['StreamSpecification']['StreamEnabled'] \
            if 'StreamSpecification' in self._table_desc else False

        if self._stream_enabled:
            self._stream_arn = self._table_desc['LatestStreamArn']
        else:
            logging.warning(f'{self.table_name} does not have Streams enabled. \n{DESCRIPTION}')

    def _generate_summary(self):
        self._summary = {
            "TableName": self.table_name,
            "TableArn": self._table_arn,
            "DeletionProtection": self._deletion_protection,
            "SizeMB": self._size_in_mb,
            "ItemCount": self._item_count,
            "BillingMode": "ON-DEMAND" if self._billing_mode == "PAY_PER_REQUEST" else "PROVISIONED"
        }
        if self._billing_mode == 'PROVISIONED':
            self._summary['ProvisionedThroughput'] = self._table_desc['ProvisionedThroughput']

        if 'GlobalSecondaryIndexes' in self._table_desc:
            self._summary['NumGSI'] = len(self._table_desc['GlobalSecondaryIndexes'])

        if 'LocalSecondaryIndexes' in self._table_desc:
            self._summary['NumLSI'] = len(self._table_desc['GlobalSecondaryIndexes'])

        if 'NumLSI' in self._table_desc or 'NumGSI' in self._table_desc:
            self._summary['IndexWarning'] = 'TODO: Indexes should be examined as well, but are not yet implemented in this program.'
            logging.warning(self._summary['IndexWarning'])

        if self._stream_enabled:
            self._summary['StreamArn'] = self._stream_arn
        self._summary['Estimations'] = self._estimations_dict['Results']

    def _describe_table(self):
        self._table_desc = self._ddb_client.describe_table(TableName=self.table_name)["Table"]
        self._table_arn = self._table_desc['TableArn']
        self._size_in_bytes = self._table_desc['TableSizeBytes']
        self._size_in_mb = ceil(self._size_in_bytes/1024/1000) if self._size_in_bytes > 1024000 else 1
        self._size_in_gb = ceil(self._size_in_bytes/1024/1000/1000) if self._size_in_bytes > 1024000000 else 1
        self._item_count = self._table_desc['ItemCount']
        if 'BillingModeSummary' in self._table_desc:
            self._billing_mode = self._table_desc['BillingModeSummary']['BillingMode']
        else:
            self._billing_mode = 'PROVISIONED'
        self._deletion_protection = self._table_desc['DeletionProtectionEnabled']
        if not self._deletion_protection:
            logging.warning(f'Deletion protection is not enabled for {self.table_name}')

    def _count_shards(self, stream_desc):
        self._stream_total_shards += len(stream_desc['Shards'])
        for s in stream_desc['Shards']:
            if 'EndingSequenceNumber' in s['SequenceNumberRange']:
                self._stream_closed_shards += 1
            else:
                self._stream_open_shards += 1
        logging.info(f'Open: {self._stream_open_shards}, Closed: {self._stream_closed_shards}, Total: {self._stream_total_shards}')

    def _describe_stream(self):
        kwargs = {
            "StreamArn": self._stream_arn
        }

        self._stream_desc = self._ddbs_client.describe_stream(**kwargs)['StreamDescription']
        self._count_shards(stream_desc=self._stream_desc)
        last_shard_id = self._stream_desc['LastEvaluatedShardId'] if 'LastEvaluatedShardId' in self._stream_desc else None
        logging.info(f'Stream: {self._stream_arn} LastEvaluatedShardId: {last_shard_id}')

        if last_shard_id is not None:
            while last_shard_id is not None:
                kwargs['ExclusiveStartShardId'] = last_shard_id
                stream_desc = self._ddbs_client.describe_stream(**kwargs)['StreamDescription']
                last_shard_id = stream_desc['LastEvaluatedShardId'] if 'LastEvaluatedShardId' in stream_desc else None
                self._count_shards(stream_desc=stream_desc)
                logging.info(f'Stream: {self._stream_arn} LastEvaluatedShardId: {last_shard_id}')

            del self._stream_desc['LastEvaluatedShardId']

        self._stream_desc['Shards'] = {
            'Open': self._stream_open_shards,
            'Closed': self._stream_closed_shards,
            'Total': self._stream_total_shards
        }

    def _generate_estimations_data(self):
        self._estimations_dict = {
            "Data": {
                "Description": "Raw estimation data used for debugging purposes only!",
                "WCU": {
                    "CurrentProvisionedWCU": self._table_desc['ProvisionedThroughput']['WriteCapacityUnits'],
                    "MaxConsumedWCU": self._metrics_data['MaxConsumedWCU'],
                    "MaxProvisionedWCU": self._metrics_data['MaxProvisionedWCU']
                },
                "RCU": {
                    "CurrentProvisionedRCU": self._table_desc['ProvisionedThroughput']['ReadCapacityUnits'],
                    "MaxConsumedRCU": self._metrics_data['MaxConsumedRCU'],
                    "MaxProvisionedRCU": self._metrics_data['MaxProvisionedRCU']
                },
                "Partitions": {}
            },
            "Results": {}
        }

        max_wcu_key = max(self._estimations_dict['Data']['WCU'], key=self._estimations_dict['Data']['WCU'].get)
        max_rcu_key = max(self._estimations_dict['Data']['RCU'], key=self._estimations_dict['Data']['RCU'].get)

        max_wcu_parts = ceil(self._estimations_dict['Data']['WCU'][max_wcu_key] / self._wcu_part_hard_limit)
        max_rcu_parts = ceil(self._estimations_dict['Data']['RCU'][max_rcu_key] / self._rcu_part_hard_limit)

        self._estimations_dict['Data']['Partitions'] = {
            max_wcu_key: max_wcu_parts if max_wcu_parts > 0 else 1,
            max_rcu_key: max_rcu_parts if max_rcu_parts > 0 else 1,
            'CurrentTableSize': ceil(self._size_in_gb/10) if self._size_in_gb > 0 else 1
        }

    def _estimate_partitions(self):
        # If the table has streams enabled, we can assume the number of open shards = partitions
        if self._stream_enabled:
            self._partitions = self._stream_open_shards
            self._estimations_dict['Results']['EstimationMethod'] = 'StreamOpenShards'
            self._estimations_dict['Results']['EstimationMethodDescription'] = 'Using number of open shards in the DynamoDB Stream, we can assume a 1:1 mapping to partitions'

        # If streams are not enabled, we need to examine a few things in order to determine the number of partitions
        else:
            max_parts_key = max(self._estimations_dict['Data']['Partitions'], key=self._estimations_dict['Data']['Partitions'].get)

            self._estimations_dict['Results']['EstimationMethod'] = max_parts_key
            self._estimations_dict['Results']['EstimationMethodDescription'] = \
                f"""{max_parts_key} was used to estimate number of partitions. 
                This value was chosen because it is the highest value from the data examined and DynamoDB tables never
                give back partitions once they've been allocated.
                The estimated partitions are calculated by taking this value, dividing by the partition limits 
                and then multiplying by 2; based on the assumption that DynamoDB is always ready to double the workload.
                """

            self._partitions = self._estimations_dict['Data']['Partitions'][max_parts_key] * 2
            # handle edge case for low utilization tables
            if self._partitions < 4 and self._billing_mode == 'PAY_PER_REQUEST':
                self._estimations_dict['Results']['EstimationMethod'] = 'OnDemandBaseSpecs'
                self._estimations_dict['Results']['EstimationMethodDescription'] = \
                    f"On-Demand Tables initially have 4 partitions and there is no data that indicates a scaling event."
                self._partitions = 4

    def _estimate_maximums(self):
        # On-Demand tables will be able to use the fully allocated previous peak capacity and partition hard limits
        if self._billing_mode == 'PAY_PER_REQUEST':
            part_comments = 'On-Demand will allow each partition to use full capacity'
            rcu_part_limit = self._rcu_part_hard_limit
            wcu_part_limit = self._wcu_part_hard_limit
            self._max_rcu = self._partitions * self._rcu_part_hard_limit
            self._max_wcu = self._partitions * self._wcu_part_hard_limit

        # Provisioned will be able to use, well, whatever is provisioned / partitions
        else:
            part_comments = f'Adaptive capacity will allow individual partitions to burst higher (up to per-partition limit or table limit, whichever is lower) based on WCU/RCU'
            if self._estimations_dict['Results']['EstimationMethod'] not in ['CurrentProvisionedWCU', 'CurrentProvisionedRCU']:
                previous_est_method = self._estimations_dict['Results']['EstimationMethod']
                self._estimations_dict['Results']['EstimationMethodDescription'] = f"""
                {previous_est_method} data indicates there are ~{self._partitions} Partitions. However, {self.table_name}'s
                current {self._billing_mode} capacity settings are limiting the overall throughput of the table and
                partitions.
                """
                self._estimations_dict['Results']['EstimationMethod'] = 'CurrentProvisionedThroughput'

            self._max_rcu = self._table_desc['ProvisionedThroughput']['ReadCapacityUnits']
            self._max_wcu = self._table_desc['ProvisionedThroughput']['WriteCapacityUnits']

            self._rcu_part_soft_limit = ceil(self._max_rcu / self._partitions)
            self._wcu_part_soft_limit = ceil(self._max_wcu / self._partitions)
            rcu_part_limit = self._rcu_part_soft_limit
            wcu_part_limit = self._wcu_part_soft_limit

        self._max_read_throughput_bytes = self._max_rcu * self._rcu_size_bytes
        self._max_write_throughput_bytes = self._max_wcu * self._wcu_size_bytes

        part_read_mbs = ceil((self._max_read_throughput_bytes / 1000000) / self._partitions)
        part_write_mbs = ceil((self._max_write_throughput_bytes / 1000000) / self._partitions)

        self._estimations_dict['Results'].update({
            'Partitions': self._partitions,
            'TableMaximums': {
                'WCU': self._max_wcu,
                'WriteThroughputMBs': ceil(self._max_write_throughput_bytes / 1000000),
                'RCU': {
                    'Base': self._max_rcu,
                    'EventuallyConsistent': self._max_rcu * self._ec_multiplier
                },
                'ReadThroughputMBs': {
                    'Base': ceil(self._max_read_throughput_bytes / 1000000),
                    'EventuallyConsistent': ceil(self._max_read_throughput_bytes / 1000000) * self._ec_multiplier
                }
            },
            'PartitionMaximums': {
                'Comments': part_comments,
                'WCU': wcu_part_limit,
                'WriteThroughputMBs': part_write_mbs,
                'RCU': {
                    'Base': rcu_part_limit,
                    'EventuallyConsistent': rcu_part_limit * self._ec_multiplier
                },
                'ReadThroughputMBs': {
                    'Base': part_read_mbs,
                    'EventuallyConsistent': part_read_mbs * self._ec_multiplier
                }
            }
        })

    def _generate_analysis(self):
        if self._verbose:
            analysis = {
                "TableDescription": self._table_desc,
                "StreamDescription": self._stream_desc,
                "EstimationData": self._estimations_dict['Data'],
                "Summary": self._summary
            }
        else:
            analysis = {
                "Summary": self._summary
            }

        self.analysis = json_dumps_iso(analysis)

    def print_analysis(self):
        print(self.analysis)

    def _get_metric_data(self, consumed_s=900, provisioned_s=3600):
        start_time = datetime.today() - timedelta(days=91)
        end_time = datetime.today() - timedelta(days=1)
        cw_paginator = self._cw_client.get_paginator('get_metric_data')

        pages = cw_paginator.paginate(
            MetricDataQueries=[
                {
                    'Id': 'cwcu',
                    'MetricStat': {
                        'Metric': {
                            'Namespace': 'AWS/DynamoDB',
                            'MetricName': 'ConsumedWriteCapacityUnits',
                            'Dimensions': [
                                {
                                    'Name': 'TableName',
                                    'Value': self.table_name
                                },
                            ]
                        },
                        'Period': consumed_s,
                        'Stat': 'Sum'
                    },
                    'ReturnData': True
                },
                {
                    'Id': 'crcu',
                    'MetricStat': {
                        'Metric': {
                            'Namespace': 'AWS/DynamoDB',
                            'MetricName': 'ConsumedReadCapacityUnits',
                            'Dimensions': [
                                {
                                    'Name': 'TableName',
                                    'Value': self.table_name
                                },
                            ]
                        },
                        'Period': consumed_s,
                        'Stat': 'Sum'
                    },
                    'ReturnData': True
                },
                {
                    'Id': 'pwcu',
                    'MetricStat': {
                        'Metric': {
                            'Namespace': 'AWS/DynamoDB',
                            'MetricName': 'ProvisionedWriteCapacityUnits',
                            'Dimensions': [
                                {
                                    'Name': 'TableName',
                                    'Value': self.table_name
                                },
                            ]
                        },
                        'Period': provisioned_s,
                        'Stat': 'Maximum'
                    },
                    'ReturnData': True
                },
                {
                    'Id': 'prcu',
                    'MetricStat': {
                        'Metric': {
                            'Namespace': 'AWS/DynamoDB',
                            'MetricName': 'ProvisionedReadCapacityUnits',
                            'Dimensions': [
                                {
                                    'Name': 'TableName',
                                    'Value': self.table_name
                                },
                            ]
                        },
                        'Period': provisioned_s,
                        'Stat': 'Maximum'
                    },
                    'ReturnData': True
                }
            ],
            StartTime=start_time,
            EndTime=end_time
        )
        max_cwcu_per_period = []
        max_crcu_per_period = []
        max_pwcu_per_period = []
        max_prcu_per_period = []
        # We are looking for the max consumed and provisioned [WR]CU in order to guesstimate number of partitions
        for page in pages:
            for results in page['MetricDataResults']:
                if results['Id'] == 'cwcu':
                    if len(results['Values']) > 0:
                        max_cwcu_per_period.append(max(results['Values']))
                        self._metrics_data['ConsumedWCU']['Timestamps'].extend(results['Timestamps'])
                        self._metrics_data['ConsumedWCU']['Values'].extend(results['Values'])
                elif results['Id'] == 'crcu':
                    if len(results['Values']) > 0:
                        max_crcu_per_period.append(max(results['Values']))
                        self._metrics_data['ConsumedRCU']['Timestamps'].extend(results['Timestamps'])
                        self._metrics_data['ConsumedRCU']['Values'].extend(results['Values'])
                elif results['Id'] == 'pwcu':
                    if len(results['Values']) > 0:
                        max_pwcu_per_period.append(max(results['Values']))
                        self._metrics_data['ProvisionedWCU']['Timestamps'].extend(results['Timestamps'])
                        self._metrics_data['ProvisionedWCU']['Values'].extend(results['Values'])
                elif results['Id'] == 'prcu':
                    if len(results['Values']) > 0:
                        max_prcu_per_period.append(max(results['Values']))
                        self._metrics_data['ProvisionedRCU']['Timestamps'].extend(results['Timestamps'])
                        self._metrics_data['ProvisionedRCU']['Values'].extend(results['Values'])

        self._metrics_data['MaxConsumedWCU'] = ceil(max(max_cwcu_per_period)/consumed_s)
        self._metrics_data['MaxConsumedRCU'] = ceil(max(max_crcu_per_period)/consumed_s)
        self._metrics_data['MaxProvisionedWCU'] = ceil(max(max_pwcu_per_period)) if len(max_pwcu_per_period) > 0 else 0
        self._metrics_data['MaxProvisionedRCU'] = ceil(max(max_prcu_per_period)) if len(max_prcu_per_period) > 0 else 0
        logging.debug(self._metrics_data)
