// this module checked local aws setup
// TODO please remove it after DynamoDB implementation
//
// local setup:
// http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html
// $ java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb -port 4567

var async = require('async');
var AWS = require('aws-sdk');

AWS.config.update({
  accessKeyId: 'some-id',
  secretAccessKey: 'some-key',
  region: 'us-east-1',
  endpoint: 'http://localhost:4567'
});

var client = new AWS.DynamoDB();
var dynamodbDoc = new AWS.DynamoDB.DocumentClient(client);

var queryOperationCount = 100;
var startedDate = new Date();
async.series({
  createTable: function(done) {
    var params = {
      TableName: 'MyTable',
      KeySchema: [
        {
          AttributeName: 'id',
          KeyType: 'HASH'
        }
      ],
      AttributeDefinitions: [
        {
          AttributeName: 'id',
          AttributeType: 'S'
        }
      ],
      ProvisionedThroughput: {
        ReadCapacityUnits: 5,
        WriteCapacityUnits: 5
      }
    };
    client.describeTable({ TableName: params.TableName }, function (err, data) {
      if ((err && err.code !== 'ResourceNotFoundException') ||
        (data && data.Table && data.Table.TableStatus === 'ACTIVE')) {
        return done();
      }
      client.createTable(params, done);
    });
  },
  putItem: function(done) {
    var params = {
      TableName: 'MyTable',
      Item: {
        id: { S: '604275e8-17de-4582-bbc2-1b5eaf3b0a4c' },
        date: { B: (new Date()).toISOString() }
      }
    };
    client.putItem(params, done);
  },
  queryBench: function(done) {
    async.times(queryOperationCount, function(n, next) {
      var key = '604275e8-17de-4582-bbc2-1b5eaf3b0a4c';
      var params = {
        TableName: 'MyTable',
        KeyConditionExpression: '#id = :key',
        ExpressionAttributeNames: {
          '#id': 'id'
        },
        ExpressionAttributeValues: {
          ':key': key
        }
      };
      dynamodbDoc.query(params, next);
    }, done);
  }
}, function(error) {
  if(error) {
    console.error('Local DynamoDB has a problem.');
    console.error(JSON.stringify(error, null, 2));
  } else {
    console.log('Local DynamoDB is operational.');
    console.log('Opration count: ' + queryOperationCount);
    console.log('Time: ' + (new Date() - startedDate) + ' ms.');
  }
});
