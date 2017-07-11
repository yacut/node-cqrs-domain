'use strict';

var util = require('util'),
  Bumper = require('../base'),
  _ = require('lodash'),
  uuid = require('uuid/v1'),
  async = require('async'),
  debug = require('debug')('domain:bumperDynamodb'),
  aws = Bumper.use('aws-sdk');

function DynamoDB(options) {
  Bumper.call(this, options);

  var defaults = {
    dbName: 'domain',
    collectionName: 'commandbumper',
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

  add: function(key, ttl, callback) {
    if (!callback) {
      callback = ttl;
      ttl = this.options.ttl;
    }

    var self = this;
    var exp = (new Date(Date.now() + ttl)).toISOString();

    var params = {
      TableName: this.options.collectionName,
      Item: { id: {S: key}, expires: {S: exp} },
      ConditionExpression: 'attribute_not_exists(id)',
      ReturnConsumedCapacity: 'TOTAL',
    };
    self.client.putItem(params, function (err, data) {
      debug('add error', err);
      debug('add data', data);
      debug('add params', JSON.stringify(params, null, 2));
      if (err && err.code === 'ConditionalCheckFailedException') {
        return callback(null, false);
      }
      if (err) {
        return callback(err);
      }
      setTimeout(function () {
        var params = {
          TableName: this.options.collectionName,
          Key: { id: {S: key} }
        };
        client.deleteItem(params, function (err) {
          if (err) {
            console.error(JSON.stringify(err, null, 2));
          }
        });
      }, ttl);

      callback(null, true);
    });
  },

  clear: function (callback) {
    this.client.deleteTable({ TableName: this.options.collectionName }, callback);
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
          console.error(JSON.stringify(err, null, 2));
          self.disconnect();
        }
      });
    }, this.options.heartbeat);
  },

});

var waitForTableExists = function(client, tableName, callback) {
  var params = {
    TableName: tableName
  };
  client.waitFor('tableExists', params, callback);
};

var createTableIfNotExists = function (client, params, callback) {
  var exists = function (p, cbExists) {
    client.describeTable({ TableName: p.TableName }, function (err, data) {
      debug('describeTable', err);
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
