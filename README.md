# DynamoDB Tools

A collection of tools to help in the administration and optimization of DynamoDB tables

## Analyzer 

This program Analyzes a current table's configuration and performs the following functions.

### 1. Estimator 
The Estimator estimates a DynamoDB Table's current partition count and performance capabilities.   
Partition count is not exposed directly, but we can infer the number of partitions several ways:
1. Count the number of Open DynamoDB Stream shards, it is a 1:1 mapping of Open shards to Partitions
2. Check cloudwatch max Provisioned settings over the last 3 months
3. Check cloudwatch max WCU/RCU utilization over the last 1 month
4. Check current WCU/RCU settings on the table if Provisioned mode is currently configured    

If Streams is not enabled, this program will estimate current number of partitions based on the maximum values 
of #2-4 above.    
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
### 2. TODO: Capacity recommendations 

NOTE: This is a future state item planned, but not currently implemented.    
On-Demand capacity mode for DynamoDB allows unpredictable workloads to avoid throttling (ish), but this stability comes at a cost.    
It is significantly more expensive than Provisioned in most cases.    
There are many scenarios where On-Demand is either not necessary or a provisioned table will flat out be cheaper.   

The recommendation should inform the user of potential cost savings opportunity.     
We can present various configurations and the associated cost savings estimates and risk levels of switching to provisioned.     

For example, switching to Provisioned Throughput mode with:    
MIN/MAX Autoscaling WCU/RCU settings    
P33 MIN/P100 MAX Autoscaling WCU/RCU settings    
P50 MIN/P100 MAX Autoscaling WCU/RCU settings    
P90 MIN/P100 MAX Autoscaling WCU/RCU settings    
P100 static WCU/RCU settings    

Will provide varying degrees of cost savings with directly linked levels of risk.