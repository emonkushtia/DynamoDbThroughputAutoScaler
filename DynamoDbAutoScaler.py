from __future__ import print_function
import json
import boto3
import datetime
import sys
import time
from enum import Enum

aws_access_key_id = boto3.session.Session().get_credentials().access_key
aws_secret_access_key = boto3.session.Session().get_credentials().secret_key
aws_security_token = boto3.session.Session().get_credentials().token


class AutoScaleType(Enum):
    DefaultScaleUp = 1
    DefaultScaleDown = 2
    MinimumScaleDown = 3


dynamodb = boto3.resource('dynamodb')
tables_names = ['AppSetting',
                'BeforeOperationErrorsJson',
                'ChangeLog',
                'ChangeTrack',
                'ColumnProgprodTotalFailedRows',
                'DefaultColumnSequences',
                'DuplicateRecordLog',
                'DynamoDBIndexDefinition',
                'GrandUploadTotalCount',
                'Mao004ResponseDetail',
                'MedicalClaim',
                'Member',
                'MemberProviderEncounterCount',
                'NullRecordCount',
                'PharmacyClaim',
                'PrimaryKeyDuplicateMedicalClaim',
                'PrimaryKeyDuplicateUploadRecordAssociation',
                'ProductReference',
                'ProgramProductBatchTotalCount',
                'ProgramProductSubmissionTotalCount',
                'RecordHashKeyLog',
                'RecordStatus',
                'ResponseRowRuleFailedRecords',
                'RowRuleFailedRecords',
                'RuleValidationStatus',
                'SeedDataTableDefinitions',
                'SubmissionRecordAssociation',
                'SyncSetting',
                'UploadOrSubmissionTotalCount',
                'UploadRecordAssociation',
                'Cms999ResponseErrorLog'
                ]


class DynamoDBIndexItem(object):
    def __init__(self, dynamoDbIndexItem):
        self.DefaultReadCapacity = int(dynamoDbIndexItem["DefaultReadCapacity"])
        self.DefaultWriteCapacity = int(dynamoDbIndexItem["DefaultWriteCapacity"])
        self.IndexName = dynamoDbIndexItem["IndexName"]
        self.TableName = dynamoDbIndexItem["TableName"]


def getDynamoDbIndexDefinitions(organization_identifier):
    dynamoDBIndexDefinitions = []
    table = dynamodb.Table('{0}_{1}'.format(organization_identifier, 'DynamoDBIndexDefinition'))
    response = table.scan()
    for item in response["Items"]:
        dynamoDBIndexDefinitions.append(DynamoDBIndexItem(item))

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        for item in response["Items"]:
            dynamoDBIndexDefinitions.append(DynamoDBIndexItem(item))
    return dynamoDBIndexDefinitions


def isEc2Off(organizationIdentifier):
    ec2client = boto3.client('ec2')
    # ec2instance = ec2.Instance('i-020a3b58baf2d2f70')
    try:
        ec2instance = ec2client.describe_instances(
            Filters=[{'Name': 'tag:Name', 'Values': [organizationIdentifier + '-01']}])
        if len(ec2instance['Reservations']) > 0:
            instance = ec2instance['Reservations'][0]['Instances'][0]
            instanceState = instance['State']['Name']
            if instanceState == 'stopped' or instanceState == 'stopping':
                return True
    except:
        print('got exception to get the EC2 instance: ', sys.exc_info())
    return False


class AutoScaler(object):
    def __init__(self, dynamo_db_index_definitions, organization_identifier, auto_scale_type):
        self.dynamoDBIndexDefinitions = dynamo_db_index_definitions
        self.organizationIdentifier = organization_identifier
        self.autoScaleType = auto_scale_type

    def autoScale(self):
        for table_name in tables_names:
            try:
                table = dynamodb.Table('{0}_{1}'.format(self.organizationIdentifier, table_name))
                self._autoScaleThroughput(table)
            except:
                pass

    def _autoScaleThroughput(self, table):
        anyChange = False
        Throughput = {}
        tableThroughput = {
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
        tableIndexName = table.name.split('_')[1]
        if self.autoScaleType != AutoScaleType.MinimumScaleDown:
            try:
                dynamoDBIndexItem = \
                    [item for item in self.dynamoDBIndexDefinitions if item.IndexName == tableIndexName][0]
                tableThroughput = {
                    'ReadCapacityUnits': dynamoDBIndexItem.DefaultReadCapacity,
                    'WriteCapacityUnits': dynamoDBIndexItem.DefaultWriteCapacity
                }
            except:
                print('could not find dynamodb table item in  dynamoDBIndexDefinitions table: ', tableIndexName)

        if self._canUpdate(table.provisioned_throughput, tableThroughput, table.name, ''):
            Throughput['ProvisionedThroughput'] = tableThroughput
            anyChange = True

        globalSecondaryIndexUpdates = self._getGlobalSecondaryIndexUpdates(table)
        if len(globalSecondaryIndexUpdates) > 0:
            Throughput['GlobalSecondaryIndexUpdates'] = globalSecondaryIndexUpdates
            anyChange = True

        try:
            if anyChange:
                self._waitForUpdateOperation()
                table.update(**Throughput)
                print('Updating Table Throughput for Table: ', table.name)
        except:
            print('auto scaling failed for table: ', tableIndexName)
            print("Unexpected error:", sys.exc_info())

    def _canUpdate(self, provisioned_throughput, updatedThroughput, table_name, secIndexName):
        if self.autoScaleType == AutoScaleType.DefaultScaleUp:
            return provisioned_throughput['ReadCapacityUnits'] == 1 and provisioned_throughput['WriteCapacityUnits'] == 1 \
                   and (provisioned_throughput['ReadCapacityUnits'] != updatedThroughput['ReadCapacityUnits'] or provisioned_throughput['WriteCapacityUnits'] != updatedThroughput['WriteCapacityUnits'])
        if self.autoScaleType == AutoScaleType.DefaultScaleDown:
            return provisioned_throughput["NumberOfDecreasesToday"] < 23 \
                   and (provisioned_throughput['ReadCapacityUnits'] > 1 or provisioned_throughput['WriteCapacityUnits'] > 1) \
                   and (provisioned_throughput['ReadCapacityUnits'] != updatedThroughput['ReadCapacityUnits'] or provisioned_throughput['WriteCapacityUnits'] != updatedThroughput['WriteCapacityUnits']) \
                   and self._isIdleOneHour(provisioned_throughput) \
                   and (self._anyConsumedCapacity(table_name, secIndexName) is False)
        if self.autoScaleType == AutoScaleType.MinimumScaleDown:
            return provisioned_throughput['ReadCapacityUnits'] > 1 or provisioned_throughput['WriteCapacityUnits'] > 1

    def _getGlobalSecondaryIndexUpdates(self, table):
        globalSecondaryIndexUpdates = []
        if table.global_secondary_indexes is not None:
            for gis in table.global_secondary_indexes:
                globalIndexThroughput = {
                    'ReadCapacityUnits': 1,
                    'WriteCapacityUnits': 1
                }
                indexName = '_' + gis['IndexName'].replace(self.organizationIdentifier, '')
                if self.autoScaleType != AutoScaleType.MinimumScaleDown:
                    try:
                        dynamoDBIndexItem = [item for item in self.dynamoDBIndexDefinitions if item.IndexName == indexName][
                            0]
                        globalIndexThroughput = {
                            'ReadCapacityUnits': dynamoDBIndexItem.DefaultReadCapacity,
                            'WriteCapacityUnits': dynamoDBIndexItem.DefaultWriteCapacity
                        }
                    except:
                        print('could not find global secondary dynamodb index in  dynamoDBIndexDefinitions table:',
                              gis['IndexName'])

                if self._canUpdate(gis['ProvisionedThroughput'], globalIndexThroughput, table.name, gis['IndexName']):
                    globalSecondaryIndexUpdates.append({
                        'Update': {
                            'IndexName': gis['IndexName'],
                            'ProvisionedThroughput': globalIndexThroughput
                        }})
        return globalSecondaryIndexUpdates

    @staticmethod
    def _isIdleOneHour(provisioned_throughput):
        max_duration = datetime.timedelta(0, 0, 0, 0, 50)
        if 'LastIncreaseDateTime' in provisioned_throughput:
            t = provisioned_throughput['LastIncreaseDateTime']
            lastIncreaseDateTime = datetime.datetime(t.year, t.month, t.day, t.hour, t.minute, t.second)
            duration = datetime.datetime.now() - lastIncreaseDateTime

            if 'LastDecreaseDateTime' in provisioned_throughput:
                t = provisioned_throughput['LastDecreaseDateTime']
                lastDecreaseDateTime = datetime.datetime(t.year, t.month, t.day, t.hour, t.minute, t.second)
                if lastIncreaseDateTime > lastDecreaseDateTime and duration > max_duration:
                    print('{0} since last increase'.format(duration))
                    return True
            else:
                if duration > max_duration:
                    print('{0} since last increase'.format(duration))
                    return True
        return False

    def _anyConsumedCapacity(self, table_name, secIndexName):
        cloudwatch = boto3.client('cloudwatch')

        end = datetime.datetime.utcnow()
        start = end - datetime.timedelta(minutes=20)
        if secIndexName == '':
            dimensions = [{'Name': 'TableName', 'Value': table_name}]
        else:
            dimensions = [
                {'Name': 'TableName', 'Value': table_name},
                {'Name': 'GlobalSecondaryIndexName', 'Value': secIndexName}
            ]
        try:
            data = cloudwatch.get_metric_statistics(Period=60, StartTime=start, EndTime=end,
                                                    MetricName='ConsumedWriteCapacityUnits', Namespace='AWS/DynamoDB',
                                                    Statistics=['Sum'], Dimensions=dimensions)
            if len(data['Datapoints']) > 0:
                return True
        except:
            print('Consumed Write Capacity getting Exception: ', sys.exc_info())
            return False

        return False

    def _waitForUpdateOperation(self):
        updatedTables = []
        for table_name in tables_names:
            try:
                table = dynamodb.Table('{0}_{1}'.format(self.organizationIdentifier, table_name))
                if table.table_status == 'UPDATING':
                    updatedTables.append(table)
            except:
                pass

        if len(updatedTables) < 10:
            return True

        time.sleep(10)
        self._waitForUpdateOperation()


def lambda_handler(event, context):
    organizationIdentifier = event["organizationIdentifier"]
    print('Started python auto scaling process for the the organization:', organizationIdentifier)

    if isEc2Off(organizationIdentifier):
        print('EC2 is stopped. So scaling all table down to minimum value.')
        AutoScaler([], organizationIdentifier, AutoScaleType.MinimumScaleDown).autoScale()
    else:
        dynamoDBIndexDefinitions = getDynamoDbIndexDefinitions(organizationIdentifier)
        if ('isEc2StartUpRule' in event) and event["isEc2StartUpRule"]:
            AutoScaler(dynamoDBIndexDefinitions, organizationIdentifier, AutoScaleType.DefaultScaleUp).autoScale()
        else:
            AutoScaler(dynamoDBIndexDefinitions, organizationIdentifier, AutoScaleType.DefaultScaleDown).autoScale()

    print('Successfully processed .')
    return 'Successfully processed .'

# lambda_handler({'organizationIdentifier': 'bbl26297399', 'isEc2StartUpRule': False}, None)
