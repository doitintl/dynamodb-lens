# DynamoDB Lens

A set of tools to help understand DynamoDB table performance capabilities and potential cost optimization opportunities.   

## Installation and usage

### Installation
```shell
python3 -m pip install dynamodb-lens
```
### CLI usage
```shell
usage: dynamodb_lens.cli [-h] --table_name TABLE_NAME [--save_output] [--verbose]
optional arguments:
  -h, --help            show this help message and exit
  --table_name TABLE_NAME
  --save_analysis         save the json formatted analysis to a file
  --verbose             Print the full analysis, otherwise a summary will be printed

Example:
python3 -m dynamodb_lens.cli --table_name sentences1
```

## Table Analyzer 

This library analyzes a table's current state, configuration and usage in order to estimate performance capabilities.     
A DynamoDB Table's current partition count as well as table settings will determine its performance capabilities.       
Partition count is not exposed directly, but we can infer the number of partitions in several ways:
1. Count the number of Open DynamoDB Stream shards, it is a 1:1 mapping of Open shards to Partitions
2. Check cloudwatch max Provisioned settings over the last 3 months
3. Check cloudwatch max WCU/RCU utilization over the last 1 month
4. Check current WCU/RCU settings on the table if Provisioned mode is currently configured   
5. Check the current storage utilization of the table

If Streams is not enabled, this program will estimate current number of partitions based on the maximum WCU/RCU values 
calculated from the data gathered in #2-5 above.    
DynamoDB tables never give back partitions once they've been allocated.    
This is what is meant by ["previous peak" in  the documentation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadWriteCapacityMode.html#HowItWorks.ProvisionedThroughput.Manual).

***This program is more accurate if DynamoDB Streams is enabled for the table.***   
Enabling streams is outside the scope of this program, it can be done via:    

----
 `aws dynamodb update-table --table-name <value> --stream-specification StreamEnabled=true,StreamViewType=KEYS_ONLY`   
and disabled with:    
 `aws dynamodb update-table --table-name <value> --stream-specification StreamEnabled=false`    

Please review the documentation and understand the implications of running these commands before executing them.

----

### TableAnalyzer class usage
The class can be imported and called directly if the `dynamodb_lens.cli` doesn't fit the use case    
The only required parameter is table_name, and optionally `verbose=True|False` plus already instantiated boto3 clients can be passed in.   

```python
from dynamodb_lens.analyzer import TableAnalyzer

table = TableAnalyzer(
    table_name='foo',
    verbose=False
)
table.print_analysis()
```

### TableAnalyzer.analysis 
The main purpose of TableAnalyzer is to produce a json-formatted analysis variable.

#### Syntax    

```json
{
    "TableDescription": { dict },
    "StreamDescription": { dict },
    "EstimationData": {
        "Description": "Raw estimation data used for debugging purposes only!",
        "WCU": {
            "CurrentProvisionedWCU": int,
            "MaxConsumedWCU": int,
            "MaxProvisionedWCU": int
        },
        "RCU": {
            "CurrentProvisionedRCU": int,
            "MaxConsumedRCU": int,
            "MaxProvisionedRCU": int
        },
        "Partitions": {
            "MaxProvisionedWCU": int,
            "MaxProvisionedRCU": int,
            "CurrentTableSize": int
        }
    },
    "Summary": {
        "TableName": "string",
        "TableArn": "string",
        "DeletionProtection": boolean,
        "SizeMB": int,
        "ItemCount": int,
        "BillingMode": "ON-DEMAND|PROVISIONED",
        "StreamArn": "string",
        "Estimations": {
            "EstimationMethod": "StreamOpenShards|OnDemandBaseSpecs|CurrentTableSize|CurrentProvisionedWCU|CurrentProvisionedRCU|MaxConsumedWCU|MaxConsumedRCU|MaxProvisionedWCU|MaxProvisionedRCU",
            "EstimationMethodDescription": "string",
            "Partitions": int,
            "TableMaximums": {
                "WCU": int,
                "WriteThroughputMBs": int,
                "RCU": {
                    "Base": int,
                    "EventuallyConsistent": int
                },
                "ReadThroughputMBs": {
                    "Base": int,
                    "EventuallyConsistent": int
                }
            },
            "PartitionMaximums": {
                "Comments": "string",
                "WCU": int,
                "WriteThroughputMBs": int,
                "RCU": {
                    "Base": int,
                    "EventuallyConsistent": int
                },
                "ReadThroughputMBs": {
                    "Base": int,
                    "EventuallyConsistent": int
                }
            }
        }
    }
}
```
#### Structure    
- TableDescription (dict)    
requires `self.verbose = True`   
The boto3 dynamodb client.describe_table response. See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/describe_table.html
- StreamDescription (dict)    
requires `self.verbose = True`    
The boto3 dynamodbstreams client.describe_stream response. See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodbstreams/client/describe_stream.html
- EstimationsData (dict)    
requires `self.verbose = True`    
This is the data used to estimate number of partitions *if streams are not enabled, primarily meant for debugging only*
  - WCU (dict)
  - RCU (dict)
  - Partitions (dict)
- Summary (dict)
  - TableName (string)
    - The name of the table
  - TableArn (string)
    - The ARN of the table
  - DeletionProtection (bool)
    - Is Deletion protection enabled?
  - SizeMB (int)
    - Size in Megabytes
  - ItemCount (int)
    - The number of items
  - BillingMode (str)
    - ON-DEMAND: Pay per request 
    - PROVISIONED: Pay hourly for predictable throughput
  - StreamArn (str)
    - The ARN of the stream
  - Estimations (dict)
    - EstimationMethod (string) - The method that was used to estimate the number of partitions and performance capabilities
        - StreamOpenShards    
          - When DynamoDB Streams are enabled for a table, we simply count the number of Open shards to determine partitions
        - OnDemandBaseSpecs
          - The table is in On-Demand mode and base table specs for on-demand were used for the calculations
        - CurrentTableSize
          - Each DynamoDB partition can be up to 10 GB in size, total size (GB)/10 
        - CurrentProvisionedWCU|CurrentProvisionedRCU
          - The current table specifications
        - MaxConsumedWCU|MaxConsumedRCU|MaxProvisionedWCU|MaxProvisionedRCU
          - Cloudwatch metrics  
    - EstimationMethodDescription (str)
      - Describes the EstimationMethod used and why
    - Partitions (int)
      - The number of partitions 
        - If Streams are enabled, this is 100% accurate. 
        - If we used metric data or table settings then it is an estimate.
    - TableMaximums (dict) - Based on number of partitions and current table settings, estimate the table maximums
      - WCU (int)
        - Total WriteCapacityUnits
      - WriteThroughputMbs
        - Total Write Throughput
      - RCU (dict)
        - Base (int)
          - ReadCapacityUnits for strong reads
        - EventuallyConsistent (int)
          - ReadCapacityUnits for eventually consistent reads
      - ReadThroughputMBs (int)
        - Base (int)
          - ReadThroughput in MB/s for strong reads
        - EventuallyConsistent (int)
          - ReadThroughput in MB/s for eventually consistent reads
    - PartitionMaximums (dict) - Based on number of partitions and current table settings, estimate the per-partition maximums
      - Comments (string)
        - Comments about the partition maximums
      - WCU (int)
        - Per-partition WriteCapacityUnits
      - WriteThroughputMbs (int)
        - Per-partition Write Throughput
      - RCU (dict)
        - Base (int)
          - Per-partition ReadCapacityUnits for strong reads
        - EventuallyConsistent (int)
          - Per-partition ReadCapacityUnits for eventually consistent reads
      - ReadThroughputMBs (int)
        - Base (int)
          - ReadThroughput in MB/s for strong reads
        - EventuallyConsistent (int)
          - ReadThroughput in MB/s for eventually consistent reads
          