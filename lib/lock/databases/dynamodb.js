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
          { AttributeName: 'aggregateId', KeyType: 'HASH' },
        ],
        AttributeDefinitions: [
          { AttributeName: 'aggregateId', AttributeType: 'S' },
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
        error('connect error: ' + err);
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
      debug('reserve err', err);
      debug('reserve data', JSON.stringify(data, null, 2));
      debug('reserve params', JSON.stringify(params, null, 2));
      if (callback) {
        callback(err);
      }
    });
  },

  resolve: function(aggregateId, callback) {
    var params = {
      Key: {
        aggregateId: {S: aggregateId}
      },
      TableName: this.options.collectionName,
      ReturnConsumedCapacity: 'TOTAL'
    };
    this.client.deleteItem(params, function (err, data) {
      debug('resolve err', err);
      debug('resolve data', JSON.stringify(data, null, 2));
      debug('resolve params', JSON.stringify(params, null, 2));
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
      ExpressionAttributeValues : {':aggregateId' : {S: aggregateId}}
    };
    this.client.scan(params, function (err, data) {
      debug('getAll err', err);
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

      callback(null, _.map(data.Items, function (entry) { return entry.id.S; }));
    });
  },

  clear: function (callback) {
    this.client.deleteTable({ TableName: this.options.collectionName}, callback);
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

      self.client.listTables(function (err) {
        if (graceTimer) clearTimeout(graceTimer);
        if (err) {
          console.error(err.stack || err);
          self.disconnect();
        }
      });
    }, this.options.heartbeat);
  },

});

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
      debug('create table', r);
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

  var active = function (d, cbActive) {
    var status = d.Table.TableStatus;
    async.until(
      function () { return status === 'ACTIVE'; },
      function (cbUntil) {
        client.describeTable({ TableName: d.Table.TableName }, function (err, data) {
          if (err) {
            console.error('There was an error checking ' + d.Table.TableName + ' status: ' + JSON.stringify(err, null, 2));
            cbUntil(err);
          } else {
            status = data.Table.TableStatus;
            debug('describe table', JSON.stringify(status, null, 2));
            setTimeout(function(){ cbUntil(null, data); }, 1000);
          }
        });
      },
      function (err, r) {
        if (err) {
          console.error('connect create table error: ' + err);
          return cbActive(err);
        }
        cbActive(null, r);
      });
  };

  async.compose(active, create, exists)(params, function (err, result) {
    debug('createTableIfNotExists', err, result);
    if (err) callback(err);
    else callback(null, result);
  });
};

module.exports = DynamoDB;
