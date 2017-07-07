var http = require('http');
var https = require('https');
var AWS = require('aws-sdk');

http.globalAgent.maxSockets = 100000;
https.globalAgent.maxSockets = 100000;

AWS.config.update({
  accessKeyId: 'some-id',
  secretAccessKey: 'some-key  ',
  region: 'us-east-1',
  endpoint: 'http://localhost:4567',
});

var client =  new AWS.DynamoDB();
var dynamodbDoc = new AWS.DynamoDB.DocumentClient(client);

var startedDate = new Date();
console.log('Start');
client.createTable({
  TableName: 'MyTable',
  KeySchema: [
    { AttributeName: 'id', KeyType: 'HASH' },
  ],
  AttributeDefinitions: [
    { AttributeName: 'id', AttributeType: 'S' },
  ],
  ProvisionedThroughput: {
    ReadCapacityUnits: 5,
    WriteCapacityUnits: 5
  }
}, function (err, data) {
  if(err){
    console.error('createTable', err);
    //return;
  }
  var params = {
    TableName: 'MyTable',
    Item: { id: {S:'604275e8-17de-4582-bbc2-1b5eaf3b0a4c'}, date: {B:(new Date()).toISOString()} },
    ConditionExpression: 'attribute_not_exists(id)'
  };
  client.putItem(params, function (err, data) {
    if (err) {
      console.error('Put document', err);
      return;
    }
    for (var i = 0; i < 1; i++) {
        getVolume('604275e8-17de-4582-bbc2-1b5eaf3b0a4c');
    }
    console.log('Over ' + (new Date() - startedDate) + ' ms');
  });
});


function getVolume(key) {
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
    var requestStarted = new Date();
    dynamodbDoc.query(params, function (err, data) {
        if (err) {
            console.error('Unable to query. Error:', JSON.stringify(err, null, 2));
        } else {
            console.log('DynamoDB : ' + (new Date() - requestStarted));
        }
    });
}
