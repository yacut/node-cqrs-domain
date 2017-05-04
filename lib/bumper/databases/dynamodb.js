'use strict';

var util = require('util'),
  Bumper = require('../base'),
  _ = require('lodash'),
  uuid = require('uuid/v1'),
  async = require('async'),
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
    self.client = new aws.DynamoDB(self.options.endpointConf);
    self.documentClient = new aws.DynamoDB.DocumentClient(self.client);
    self.isConnected = true;
    function collectionTableDefinition(opts) {
      var def = {
        TableName: opts.collectionName,
        KeySchema: [
          { AttributeName: 'id', KeyType: 'HASH' },
          { AttributeName: 'expires', KeyType: 'RANGE' }
        ],
        AttributeDefinitions: [
          { AttributeName: 'id', AttributeType: 'S' },
          { AttributeName: 'expires', AttributeType: 'S' }
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
    var client = new aws.DynamoDB.DocumentClient(self.options.endpointConf);
    var exp = new Date(Date.now() + ttl);

    var bumper = {
      TableName: opts.collectionName,
      Item: { id: key, expires: exp },
      ConditionExpression: 'attribute_not_exists(id)'
    };
    client.put(bumper, function (err, data) {
      if (err) {
        return callback(err);
      }
      setTimeout(function () {
        client.delete({ TableName: opts.collectionName, Key: {id: key}}, function () {});
      }, ttl);

      return callback(null, true);

      callback(null, true);
    });
  },
  // TODO ready check https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#waitFor-property

  /// FIXME
  get: function (id, callback) {
    if (!id || !_.isString(id)) {
      var err = new Error('Please pass a valid id!');
      return callback(err);
    }

    this.client.query({ id: id }, function (err, entry) {
      if (err) {
        return callback(err);
      }

      if (!entry) {
        return callback(null, null);
      }

      callback(null, entry.revision || null);
    });
  },

  /// FIXME
  set: function (id, revision, oldRevision, callback) {
    if (!id || !_.isString(id)) {
      return callback(new Error('Please pass a valid id!'));
    }
    if (!revision || !_.isNumber(revision)) {
      var err = new Error('Please pass a valid revision!');
      return callback(err);
    }

    this.client.put({ id: id, revision: oldRevision }, { id: id, revision: revision }, function (err, modifiedCount) {
      if (isNew) {
        if (modifiedCount && modifiedCount.result && modifiedCount.result.n === 0) {
          err = new ConcurrencyError();
          if (callback) {
            callback(err);
          }
          return;
        }
      } else {
        if (modifiedCount === 0) {
          err = new ConcurrencyError();
          if (callback) {
            callback(err);
          }
          return;
        }
      }
      if (err && err.message && err.message.match(/duplicate key/i)) {
        debug(err);
        err = new ConcurrencyError();
        debug(err);
        if (callback) {
          callback(err);
        }
        return;
      }
      if (callback) { callback(err); }
    });
  },

  /// FIXME
  saveLastEvent: function (evt, callback) {
    // https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#putItem-property
    this.client.putItem({ id: 'THE_LAST_SEEN_EVENT', event: evt }, function (err) {
      if (callback) { callback(err); }
    });
  },

  /// FIXME
  getLastEvent: function (callback) {
    // https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#getItem-property
    this.client.getItem({ id: 'THE_LAST_SEEN_EVENT' }, function (err, entry) {
      if (err) {
        return callback(err);
      }

      if (!entry) {
        return callback(null, null);
      }

      callback(null, entry.event || null);
    });
  },

  clear: function (callback) {
    this.client.deleteTable({ TableName: opts.collectionName}, callback);
  }

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
            setTimeout(cbUntil(null, data), 1000);
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
