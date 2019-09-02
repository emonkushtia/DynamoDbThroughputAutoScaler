# DynamoDbThroughputAutoScaler
This script for Auto Scale up and Scale down of Dynamo DB throughput. 
We use dynamo DB as a read database and our application has a ETL process which process row file and transform the data and load into our Redshift database.
A sync process initiates to sync those data to DynamoDb database. So, we develop a lambda function to scale down the throughput after each hour if there is no write activity in the dynamoDB table to optimize the cost.
Similarly when we start the EC2 then we set the throughput value to a specific value which is required to run the application smoothly.
When we stop the EC2 instance then it automatically decrease the throughput to 1. It's save huge dynamo DB cost for our application.
