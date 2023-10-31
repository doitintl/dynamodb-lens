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
  --save_output         save the json formatted output to a file
  --verbose             Print the full output, otherwise a summary will be printed

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
table.save_output()
```