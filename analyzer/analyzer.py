import logging
import os
from datetime import datetime, timedelta
from math import ceil
from utils import json_dumps_iso, return_boto3_client


DESCRIPTION = '''This program estimates a DynamoDB Table's current partition count and performance capabilities.
Partition count is not exposed directly, but we can infer the number of partitions several ways:
1. Count the number of Open DynamoDB Stream shards, it is a 1:1 mapping of Open shards to Partitions
2. Check cloudwatch max Provisioned settings over the last 3 months
3. Check cloudwatch max WCU/RCU utilization over the last 1 month
4. Check current WCU/RCU settings on the table if Provisioned mode is currently configured 
If Streams is not enabled, this program will estimate current number of partitions based on the maximum values 
of #2-4 above. DynamoDB tables never give back partitions once they've been allocated.

This program prefers DynamoDB Streams to be enabled for the --table_name in order to accurately determine number of partitions.
Enabling streams is outside the scope of this program, it can be done via:
 aws dynamodb update-table --table-name <value> --stream-specification StreamEnabled=true,StreamViewType=KEYS_ONLY
and disabled with:
 aws dynamodb update-table --table-name <value> --stream-specification StreamEnabled=false
Please review the documentation and understand the implications of running these commands before executing them.
'''

logging.basicConfig(level=os.environ.get('LOG_LEVEL', 'INFO'))


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
        self.lambda_client=lambda_client
        self.ddbs_client = ddbs_client
        self.ddb_client = ddb_client
        self.cw_client = cw_client
        self.verbose = verbose
        self.table_desc = None
        self.stream_desc = None
        self.table_arn = None
        self.stream_arn = None
        self.size_in_bytes = None
        self.size_in_mb = None
        self.item_count = None
        self.ec_multiplier = 2
        self.wcu_size_bytes = 1000
        self.rcu_size_bytes = 4000
        self.rcu_part_hard_limit = 3000
        self.wcu_part_hard_limit = 1000
        self.rcu_part_soft_limit = None
        self.wcu_part_soft_limit = None
        self.partitions = None
        self.billing_mode = None
        self.part_wcu = None
        self.part_rcu = None
        self.max_rcu = None
        self.max_read_throughput_bytes = None
        self.max_wcu = None
        self.max_write_throughput_bytes = None
        self.summary = None
        self.detailed = None
        self.output = None
        self.stream_enabled = None
        self.stream_open_shards = 0
        self.stream_closed_shards = 0
        self.stream_total_shards = 0
        self.metrics_data = {
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
        self.estimations_dict = {}

        logging.info(f'Working on table: {self.table_name}')
        self.describe_table()
        self.is_stream_enabled()
        logging.info(f'Working on stream: {self.stream_arn}')
        if self.stream_enabled:
            self.describe_stream()
        logging.info(f'Pulling Cloudwatch metrics for table: {self.table_name}')
        self.get_metric_data()
        logging.info(f'Crunching numbers...')
        self.generate_estimations_data()
        self.estimate_partitions()
        self.estimate_maximums()
        self.generate_summary()
        if self.verbose:
            self.generate_verbose()
        self.generate_output()

    def is_stream_enabled(self):
        self.stream_enabled = self.table_desc['StreamSpecification']['StreamEnabled'] \
            if 'StreamSpecification' in self.table_desc else False

        if self.stream_enabled:
            self.stream_arn = self.table_desc['LatestStreamArn']
        else:
            logging.warning(f'{self.table_name} does not have Streams enabled. \n{DESCRIPTION}')

    def generate_summary(self):
        self.summary = {
            'Name': self.table_name,
            'Arn': self.table_arn,
            'SizeMB': self.size_in_mb,
            'ItemCount': self.item_count,
            'BillingMode': self.billing_mode,
            'Estimations': self.estimations_dict['Results']
        }

    def generate_verbose(self):
        self.detailed = {
            "TableDescription": self.table_desc,
            "StreamDescription": self.stream_desc,
            "EstimationData": self.estimations_dict['Data'],
            "AnalyzerSummary": self.summary,
        }

    def describe_table(self):
        self.table_desc = self.ddb_client.describe_table(TableName=self.table_name)["Table"]
        self.table_arn = self.table_desc['TableArn']
        self.size_in_bytes = self.table_desc['TableSizeBytes']
        self.size_in_mb = ceil(self.size_in_bytes / 1024/1000) if self.size_in_bytes > 1024000 else 1
        self.item_count = self.table_desc['ItemCount']
        self.billing_mode = self.table_desc['BillingModeSummary']['BillingMode']

    def count_shards(self, stream_desc):
        self.stream_total_shards += len(stream_desc['Shards'])
        for s in stream_desc['Shards']:
            if 'EndingSequenceNumber' in s['SequenceNumberRange']:
                self.stream_closed_shards += 1
            else:
                self.stream_open_shards += 1
        logging.info(f'Open: {self.stream_open_shards}, Closed: {self.stream_closed_shards}, Total: {self.stream_total_shards}')

    def describe_stream(self):
        kwargs = {
            "StreamArn": self.stream_arn
        }

        self.stream_desc = self.ddbs_client.describe_stream(**kwargs)['StreamDescription']
        self.count_shards(stream_desc=self.stream_desc)
        last_shard_id = self.stream_desc['LastEvaluatedShardId'] if 'LastEvaluatedShardId' in self.stream_desc else None
        logging.debug(f'Stream: {self.stream_arn} LastEvaluatedShardId: {last_shard_id}')

        if last_shard_id is not None:
            while last_shard_id is not None:
                kwargs['ExclusiveStartShardId'] = last_shard_id
                stream_desc = self.ddbs_client.describe_stream(**kwargs)['StreamDescription']
                last_shard_id = stream_desc['LastEvaluatedShardId'] if 'LastEvaluatedShardId' in stream_desc else None
                self.count_shards(stream_desc=stream_desc)
                logging.debug(f'Stream: {self.stream_arn} LastEvaluatedShardId: {last_shard_id}')

            del self.stream_desc['LastEvaluatedShardId']

        self.stream_desc['Shards'] = {
            'Open': self.stream_open_shards,
            'Closed': self.stream_closed_shards,
            'Total': self.stream_total_shards
        }

    def generate_estimations_data(self):
        self.estimations_dict = {
            "Data": {
                "WCU": {
                    "CurrentProvisionedWCU": self.table_desc['ProvisionedThroughput']['WriteCapacityUnits'],
                    "MaxConsumedWCU": self.metrics_data['MaxConsumedWCU'],
                    "MaxProvisionedWCU": self.metrics_data['MaxProvisionedWCU']
                },
                "RCU": {
                    "CurrentProvisionedRCU": self.table_desc['ProvisionedThroughput']['ReadCapacityUnits'],
                    "MaxConsumedRCU": self.metrics_data['MaxConsumedRCU'],
                    "MaxProvisionedRCU": self.metrics_data['MaxProvisionedRCU']
                },
                "Partitions": {}
            },
            "Results": {}
        }

        max_wcu_key = max(self.estimations_dict['Data']['WCU'], key=self.estimations_dict['Data']['WCU'].get)
        max_rcu_key = max(self.estimations_dict['Data']['RCU'], key=self.estimations_dict['Data']['RCU'].get)

        max_wcu_parts = ceil(self.estimations_dict['Data']['WCU'][max_wcu_key] / self.wcu_part_hard_limit)
        max_rcu_parts = ceil(self.estimations_dict['Data']['RCU'][max_rcu_key] / self.rcu_part_hard_limit)

        self.estimations_dict['Data']['Partitions'] = {
            max_wcu_key: max_wcu_parts if max_wcu_parts > 0 else 1,
            max_rcu_key: max_rcu_parts if max_rcu_parts > 0 else 1
        }

    def estimate_partitions(self):
        # If the table has streams enabled, we can assume the number of open shards = partitions
        if self.stream_enabled:
            self.partitions = self.stream_open_shards
            self.estimations_dict['Results']['EstimationMethod'] = 'StreamOpenShards'
            self.estimations_dict['Results']['EstimationMethodDescription'] = 'Using number of open shards in the DynamoDB Stream, we can assume a 1:1 mapping to partitions'

        # If streams are not enabled, we need to examine a few things in order to determine the number of partitions
        # 1. If Provisioned, current capacity settings
        # 2. Previous RCU/WCU Consumption (30 days)
        # 3. Previous RCU/WCU Provisioned (90 days)
        # The greater of these numbers will be used to determine number of partitions
        # NOTE: This method is only as accurate as the data we have access to.
        #       EG: If there was a major scale-up event more than 90 days ago we will not be able to factor it in
        else:
            max_parts_key = max(self.estimations_dict['Data']['Partitions'], key=self.estimations_dict['Data']['Partitions'].get)

            self.estimations_dict['Results']['EstimationMethod'] = max_parts_key
            self.estimations_dict['Results']['EstimationMethodDescription'] = \
                f"""{max_parts_key} was used to estimate number of partitions. 
                This value was chosen because it is the highest value from the data examined and DynamoDB tables never
                give back partitions once they've been allocated.
                The estimated partitions are calculated by taking this value, dividing by the partition limits 
                and then multiplying by 2; based on the assumption that DynamoDB is always ready to double the workload.
                """

            self.partitions = self.estimations_dict['Data']['Partitions'][max_parts_key] * 2
            # handle edge case for low utilization tables
            if self.partitions < 4 and self.billing_mode == 'PAY_PER_REQUEST':
                self.estimations_dict['Results']['EstimationMethod'] = 'OnDemandBaseSpecs'
                self.estimations_dict['Results']['EstimationMethodDescription'] = \
                    f"On-Demand Tables initially have 4 partitions and there is no data that indicates a scaling event."
                self.partitions = 4

    def estimate_maximums(self):
        # On-Demand tables will be able to use the fully allocated previous peak capacity and partition hard limits
        if self.billing_mode == 'PAY_PER_REQUEST':
            part_comments = 'On-Demand will allow each partition to use full capacity'
            rcu_part_limit = self.rcu_part_hard_limit
            wcu_part_limit = self.wcu_part_hard_limit
            self.max_rcu = self.partitions * self.rcu_part_hard_limit
            self.max_wcu = self.partitions * self.wcu_part_hard_limit

        # Provisioned will be able to use, well, whatever is provisioned / partitions
        else:
            part_comments = f'Adaptive capacity will allow individual partitions to burst higher (up to per-partition limit or table limit, whichever is lower) based on WCU/RCU'
            self.max_rcu = self.table_desc['ProvisionedThroughput']['ReadCapacityUnits']
            self.max_wcu = self.table_desc['ProvisionedThroughput']['WriteCapacityUnits']
            self.rcu_part_soft_limit = ceil(self.max_rcu / self.partitions)
            self.wcu_part_soft_limit = ceil(self.max_wcu / self.partitions)
            rcu_part_limit = self.rcu_part_soft_limit
            wcu_part_limit = self.wcu_part_soft_limit

        self.max_read_throughput_bytes = self.max_rcu * self.rcu_size_bytes
        self.max_write_throughput_bytes = self.max_wcu * self.wcu_size_bytes

        part_read_mbs = ceil((self.max_read_throughput_bytes / 1000000) / self.partitions)
        part_write_mbs = ceil((self.max_write_throughput_bytes / 1000000) / self.partitions)

        self.estimations_dict['Results'].update({
            'Partitions': self.partitions,
            'TableMaximums': {
                'WCU': self.max_wcu,
                'WriteThroughputMBs': ceil(self.max_write_throughput_bytes / 1000000),
                'RCU': {
                    'Base': self.max_rcu,
                    'EventuallyConsistent': self.max_rcu * self.ec_multiplier
                },
                'ReadThroughputMBs': {
                    'Base': ceil(self.max_read_throughput_bytes / 1000000),
                    'EventuallyConsistent': ceil(self.max_read_throughput_bytes / 1000000) * self.ec_multiplier
                }
            },
            'PartitionMaximums': {
                'Comments': part_comments,
                'WCU': wcu_part_limit,
                'WriteThroughputMBs': part_write_mbs,
                'RCU': {
                    'Base': rcu_part_limit,
                    'EventuallyConsistent': rcu_part_limit * self.ec_multiplier
                },
                'ReadThroughputMBs': {
                    'Base': part_read_mbs,
                    'EventuallyConsistent': part_read_mbs * self.ec_multiplier
                }
            }
        })

    def generate_output(self):
        if self.verbose:
            self.output = json_dumps_iso(self.detailed)
        else:
            self.output = json_dumps_iso(self.summary)

    def get_metric_data(self, consumed_s=900, provisioned_s=3600):
        start_time = datetime.today() - timedelta(days=91)
        end_time = datetime.today() - timedelta(days=1)
        cw_paginator = self.cw_client.get_paginator('get_metric_data')

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
                        self.metrics_data['ConsumedWCU']['Timestamps'].extend(results['Timestamps'])
                        self.metrics_data['ConsumedWCU']['Values'].extend(results['Values'])
                elif results['Id'] == 'crcu':
                    if len(results['Values']) > 0:
                        max_crcu_per_period.append(max(results['Values']))
                        self.metrics_data['ConsumedRCU']['Timestamps'].extend(results['Timestamps'])
                        self.metrics_data['ConsumedRCU']['Values'].extend(results['Values'])
                elif results['Id'] == 'pwcu':
                    if len(results['Values']) > 0:
                        max_pwcu_per_period.append(max(results['Values']))
                        self.metrics_data['ProvisionedWCU']['Timestamps'].extend(results['Timestamps'])
                        self.metrics_data['ProvisionedWCU']['Values'].extend(results['Values'])
                elif results['Id'] == 'prcu':
                    if len(results['Values']) > 0:
                        max_prcu_per_period.append(max(results['Values']))
                        self.metrics_data['ProvisionedRCU']['Timestamps'].extend(results['Timestamps'])
                        self.metrics_data['ProvisionedRCU']['Values'].extend(results['Values'])

        self.metrics_data['MaxConsumedWCU'] = ceil(max(max_cwcu_per_period)/consumed_s)
        self.metrics_data['MaxConsumedRCU'] = ceil(max(max_crcu_per_period)/consumed_s)
        self.metrics_data['MaxProvisionedWCU'] = ceil(max(max_pwcu_per_period)) if len(max_pwcu_per_period) > 0 else 0
        self.metrics_data['MaxProvisionedRCU'] = ceil(max(max_prcu_per_period)) if len(max_prcu_per_period) > 0 else 0
        logging.debug(self.metrics_data)
