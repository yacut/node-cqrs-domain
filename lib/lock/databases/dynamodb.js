'use strict';

var util = require('util'),
  Bumper = require('../base'),
  _ = require('lodash'),
  uuid = require('uuid/v1'),
  async = require('async'),
  debug = require('debug')('domain:lockDynamodb'),
  aws = Bumper.use('aws-sdk');

function DynamoDB(options) {
  Bumper.call(this, options);

  var defaults = {
    dbName: 'domain',
    collectionName: 'aggregatelock',
    ttl:  1000 * 60 * 60 * 1, // 1 hour
    endpointConf: {
      endpoint: 'http://localhost:4567' //dynalite
    },
    // heartbeat: 1000
  };
  if (process.env['AWS_DYNAMODB_ENDPOINT']) {
    defaults.endpointConf = process.env['AWS_DYNAMODB_ENDPOINT'];
  }
  debug('region', aws.config.region);
  if(!aws.config.region){
    aws.config.update({region: 'us-east-1'});
  }
  _.defaults(options, defaults);
  this.options = options;
}

util.inherits(DynamoDB, Bumper);

_.extend(DynamoDB.prototype, {

  connect: function (callback) {
    var self = this;
    self.client = new aws.DynamoDB();
    debug('Client created.');
    function collectionTableDefinition(opts) {
      var def = {
        TableName: opts.collectionName,
        KeySchema: [
          { AttributeName: 'id', KeyType: 'HASH' },
        ],
        AttributeDefinitions: [
          { AttributeName: 'id', AttributeType: 'S' },
        ],
        ProvisionedThroughput: {
          ReadCapacityUnits: opts.EventsReadCapacityUnits || 5,
          WriteCapacityUnits: opts.EventsWriteCapacityUnits || 5
        }
      };

      return def;
    }
    createTableIfNotExists(self.client, collectionTableDefinition(self.options), function (err) {
      if (err) {
        console.error('connect error: ' + err);
        if (callback) callback(err);
      } else {
        self.emit('connect');
        self.isConnected = true;
        debug('Connected.');

        if (self.options.heartbeat) {
          self.startHeartbeat();
        }
        if (callback) callback(null, self);
      }
    });
  },

  disconnect: function (callback) {
    this.emit('disconnect');
    debug('Disconnected.');
    if (callback) callback(null);
  },

  getNewId: function(callback) {
    callback(null, uuid());
  },

  reserve: function(workerId, aggregateId, callback) {
    var params = {
      TableName: this.options.collectionName,
      Item: { id: {S: workerId}, aggregateId: {S: aggregateId}, date: {S: (new Date()).toISOString()} },
      ConditionExpression: 'attribute_not_exists(id)',
      ReturnConsumedCapacity: 'TOTAL'
    };
    this.client.putItem(params, function (err, data) {
      debug('reserve err', JSON.stringify(err, null, 2));
      debug('reserve data', JSON.stringify(data, null, 2));
      debug('reserve params', JSON.stringify(params, null, 2));
      if (callback) {
        callback(err);
      }
    });
  },

  resolve: function(aggregateId, callback) {
    var self = this;
    async.waterfall([
      function(done) {
        self.getAll(aggregateId, done);
      },
      function(ids, done) {
        async.each(ids, function(id, next) {
          var params = {
            Key: {
              id: {S: id}
            },
            TableName: self.options.collectionName,
            ReturnConsumedCapacity: 'TOTAL'
          };
          self.client.deleteItem(params, next);
        }, done);
      }
    ], function (err, data) {
      debug('resolve err', JSON.stringify(err, null, 2));
      debug('resolve data', JSON.stringify(data, null, 2));
      if(err && err.code === 'ResourceNotFoundException') {
        return callback(null, undefined);
      }
      if (callback) {
        callback(err);
      }
    });
  },

  getAll: function(aggregateId, callback) {
    var params = {
      TableName: this.options.collectionName,
      FilterExpression : 'aggregateId = :aggregateId',
      ExpressionAttributeValues : {':aggregateId' : {S: aggregateId}},
      ProjectionExpression: 'id, aggregateId'
    };
    this.client.scan(params, function (err, data) {
      debug('getAll err', JSON.stringify(err, null, 2));
      debug('getAll data', JSON.stringify(data, null, 2));
      debug('getAll params', JSON.stringify(params, null, 2));
      if(err && err.code === 'ResourceNotFoundException') {
        return callback(null, []);
      }
      if (err) {
        return callback(err);
      }

      if (!data|| !data.Items) {
        return callback(null, []);
      }
      callback(null, _.map(data.Items, function (entry) { return entry.id.S; }).sort());
    });
  },

  clear: function (callback) {
    var self = this;
    async.waterfall([
      function(done){
        self.client.scan({ TableName: self.options.collectionName }, done);
      },
      function(data, done){
        if(!data || !data.Items || data.Count === 0) {
          return done();
        }
        async.each(data.Items, function(entry, next){
          debug('delete item', JSON.stringify(entry, null, 2));
          var params = {
            Key: {
              id: {S: entry.id.S}
            },
            TableName: self.options.collectionName,
            ReturnConsumedCapacity: 'TOTAL'
          };
          self.client.deleteItem(params, next);
        }, done);
      }
    ], callback);
  },

  stopHeartbeat: function () {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      delete this.heartbeatInterval;
    }
  },

  startHeartbeat: function () {
    var self = this;

    var gracePeriod = Math.round(this.options.heartbeat / 2);
    this.heartbeatInterval = setInterval(function () {
      var graceTimer = setTimeout(function () {
        if (self.heartbeatInterval) {
          console.error((new Error ('Heartbeat timeouted after ' + gracePeriod + 'ms (dynamodb)')).stack);
          self.disconnect();
        }
      }, gracePeriod);

      waitForTableExists(self.client, self.options.collectionName, function (err) {
        if (graceTimer) clearTimeout(graceTimer);
        if (err) {
          console.error(err.stack || err);
          self.disconnect();
        }
      });
    }, this.options.heartbeat);
  },

});

var waitForTableExists = function(client, tableName, callback) {
  debug('Wating for table:', tableName);
  var params = {
    TableName: tableName
  };
  client.waitFor('tableExists', params, callback);
};

var createTableIfNotExists = function (client, params, callback) {
  var exists = function (p, cbExists) {
    client.describeTable({ TableName: p.TableName }, function (err, data) {
      if (err) {
        if (err.code === 'ResourceNotFoundException') {
          cbExists(null, { exists: false, definition: p });
        } else {
          console.error('Table ' + p.TableName + ' doesn\'t exist yet but describeTable: ' + JSON.stringify(err, null, 2));
          cbExists(err);
        }
      } else {
        cbExists(null, { exists: true, description: data });
      }
    });
  };

  var create = function (r, cbCreate) {
    if (!r.exists) {
      client.createTable(r.definition, function (err, data) {
        if (err) {
          console.error('Error while creating ' + r.definition.TableName + ': ' + JSON.stringify(err, null, 2));
          cbCreate(err);
        } else {
          cbCreate(null, { Table: { TableName: data.TableDescription.TableName, TableStatus: data.TableDescription.TableStatus } });
        }
      });
    } else {
      cbCreate(null, r.description);
    }
  };

  async.parallel([
    function(done) {
      async.compose(create, exists)(params, done);
    },
    function(done){
      waitForTableExists(client, params.TableName, done);
    }
  ], function (err) {
    debug('createTableIfNotExists', JSON.stringify(err, null, 2));
    if (err) callback(err);
    else callback(null);
  });
};

module.exports = DynamoDB;
