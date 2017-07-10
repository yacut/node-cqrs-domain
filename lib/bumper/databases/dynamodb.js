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
  };
  if (process.env['AWS_DYNAMODB_ENDPOINT']) {
    defaults.endpointConf = process.env['AWS_DYNAMODB_ENDPOINT'];
  }
  _.defaults(options, defaults);
  this.options = options;
}

util.inherits(DynamoDB, Bumper);

_.extend(DynamoDB.prototype, {

  connect: function (callback) {
    var self = this;
    self.client = new aws.DynamoDB();
    self.isConnected = true;
    debug('Client initialized.');
    function collectionTableDefinition(opts) {
      var def = {
        TableName: opts.collectionName,
        KeySchema: [
          { AttributeName: '_id', KeyType: 'HASH' },
        ],
        AttributeDefinitions: [
          { AttributeName: '_id', AttributeType: 'S' },
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
      Item: { _id: {S: key}, expires: {S: exp} },
      ConditionExpression: 'attribute_not_exists(_id)',
      ReturnConsumedCapacity: 'TOTAL',
    };
    self.client.putItem(params, function (err, data) {
      debug('add error', err);
      debug('add data', data);
      debug('add params', JSON.stringify(params, null, 2));
      if (err && err.code && err.code === 'ConditionalCheckFailedException') {
        return callback(null, false);
      }
      if (err) {
        return callback(err);
      }
      setTimeout(function () {
        var params = {
          TableName: this.options.collectionName,
          Key: { _id: {S: key} }
        };
        client.deleteItem(params, function (err) { debug('Delete Item failed.', err); });
      }, ttl);

      callback(null, true);
    });
  },

  clear: function (callback) {
    this.client.deleteTable({ TableName: this.options.collectionName }, callback);
  }

});

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
        debug('createTable', err);
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
          debug('describeTable', err);
          if (err) {
            console.error('There was an error checking ' + d.Table.TableName + ' status: ' + JSON.stringify(err, null, 2));
            cbUntil(err);
          } else {
            status = data.Table.TableStatus;
            setTimeout(function() { cbUntil(null, data); }, 1000);
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
    if (err) callback(err);
    else callback(null, result);
  });
};

module.exports = DynamoDB;
