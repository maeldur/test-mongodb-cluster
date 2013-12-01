var async = require('async');
var cp = require('child_process');
var mkdirp = require('mkdirp');
var mongodb = require('mongodb');
var path = require('path');

var MongoClient = require('mongodb').MongoClient;

var ServerType = {
  MONGOS: 1,
  MONGOCFG: 2,
  MONGOD: 3
};

var ServerLabels = {
  1: 'mongos',
  2: 'mongocfg',
  3: 'mongod'
}

//wraps a mongo instance.  can be many types.
var MongoServer = function(options) {
  this.type = options.type;
  this.port = options.port;
  this.replSetName = options.replSetName;
  this.rootDataDir = options.dataDir;
  this.rootLogDir = options.logDir;
  this.configPorts = options.configPorts || [];
};

MongoServer.prototype.start = function(callback) {
  var executable = 'mongod';
  var commandArgs = [];

  if (this.type === ServerType.MONGOS) {
    executable = 'mongos';
  } else if (this.type === ServerType.MONGOCFG) {
    commandArgs.push('--configsvr');
  } else if (this.type === ServerType.MONGOD) {
    if (!!this.replSetName) {
      commandArgs.push('--replSet');
      commandArgs.push(this.replSetName);
    }
  }

  if (this.type !== ServerType.MONGOS) {
    commandArgs.push('--dbpath');
    commandArgs.push(this.dataDir);
  } else {
    commandArgs.push('--configdb');
    commandArgs.push(this.configPorts.map(function(port) {
      return '127.0.0.1:' + port;
    }).join(','));
  }

  commandArgs.push('--port');
  commandArgs.push(this.port);
  commandArgs.push('--logpath');
  commandArgs.push(
    path.resolve(
      this.rootLogDir,
      ServerLabels[this.type] + '_' + this.port + '.log'
    )
  );
  console.log('start command: ' + executable + ' ' + commandArgs.join(' '));
  this.process = cp.spawn(executable, commandArgs);
  this.process.stdout.on('data', function(data) {
    console.log('stdout: ' + data);
  });
  this.process.stderr.on('data', function(data) {
    console.log('stderr: ' + data);
  });
  this.process.on('close', function(code) {
    console.log('child process exited ' + code);
  });

  callback();
};

MongoServer.prototype.createDirectories = function(callback) {
  var dataDirParts = [ServerLabels[this.type]];
  dataDirParts.push(this.port);
  if (!!this.replSetName) {
    dataDirParts.push(this.replSetName);
  }
  this.dataDir = path.resolve(this.rootDataDir, dataDirParts.join('_'));
  mkdirp(this.dataDir, function(err) {
    return callback(err);
  });
};


function checkRequirements() {
  try {
    require('optimist');
    cp.exec('which mongod', function(err, stdout, stderr) {
      console.log('Using: ' + stdout);
    });
  } catch (e) {
    return false;
  }
  return true;
}

function processOptions(argv) {
  var options = {
    port: 26000,
    sharded: false,
    replicated: false,
    dataDir: path.resolve('./data'),
    logDir: path.resolve('./logs')
  };

  if (argv.port) {
    options.port = argv.port;
  }

  if (argv.sharded) {
    options.sharded = true;
    options.shardCount = 1;
    options.configServerCount = 1;
    if (argv.shardCount) {
      options.shardCount = parseInt(argv.shardCount, 10);
    }
    if (argv.configServerCount) {
      options.configServerCount = parseInt(argv.configServerCount, 10);
    }
  }

  if (argv.replicated) {
    options.replicated = true;
    options.replMemberCount = 3;
    options.replSetName = 'test';
    if (argv.replMemberCount) {
      options.replMemberCount = parseInt(argv.replMemberCount, 10);
    }
    if (argv.replSetName) {
      options.replSetName = argv.replSetName;
    }
  }

  if (argv.dataDir) {
    options.dataDir = path.resolve(argv.dataDir);
  }

  return options;
}

function start(options) {
  console.log('starting cluster configuration: ', options);
  var servers = [];
  var replicaSets = {};

  var port = options.port;
  var startPort = port;

  var shardCount = options.shardCount || 1;

  // start non-mongos on port+1
  if (options.sharded) {
    port++;
  }

  for (var s = 0; s < shardCount; s++) {
    if (options.replicated) {
      // only a replica set
      for (var i = 0; i < options.replMemberCount; i++) {
        var replSetName = options.replSetName + (options.sharded ? s : '');
        servers.push(new MongoServer({
          dataDir: options.dataDir,
          logDir: options.logDir,
          type: ServerType.MONGOD,
          replSetName: replSetName,
          port: port
        }));
        if (!replicaSets[replSetName]) {
          replicaSets[replSetName] = {
            members: []
          }
        }
        replicaSets[replSetName].members.push(port);
        port++;
      }
    } else {
      // single mongod
      servers.push(new MongoServer({
        dataDir: options.dataDir,
        logDir: options.logDir,
        type: ServerType.MONGOD,
        port: port++,
      }));
    }
  }

  var launchGroupOne, launchGroupTwo;

  if (options.sharded) {
    // add config servers
    var configPorts = [];
    for (var c = 0; c < options.configServerCount; c++) {
      configPorts.push(port);
      servers.splice(0, 0, new MongoServer({
        dataDir: options.dataDir,
        logDir: options.logDir,
        type: ServerType.MONGOCFG,
        port: port++
      }));
    }

    servers.splice(0, 0, new MongoServer({
      dataDir: options.dataDir,
      logDir: options.logDir,
      type: ServerType.MONGOS,
      port: startPort,
      configPorts: configPorts
    }));

    launchGroupOne = servers.slice(1);
    launchGroupTwo = servers.slice(0, 1);
  } else {
    launchGroupOne = servers;
    launchGroupTwo = [];
  }

  async.waterfall([
    function createDirectories(cb) {
      // TODO: ensure permissions
      mkdirp(path.resolve(options.logDir), function(err) {
        if (err) { return cb(err); }
        mkdirp(path.resolve(options.dataDir), function(err) {
          if (err) { return cb(err); }
          async.forEach(servers, function(server, cb) {
            server.createDirectories(cb);
          }, cb);
        });
      });
    },
    function launchOne(cb) {
      async.forEach(launchGroupOne, function(server, cb) {
        server.start(cb);
      }, cb);
    },
    function waitForServers1(cb) {
      console.log('waiting for group one servers to start..');
      async.forEach(launchGroupOne, function(server, cb) {
        var connected = false;
        async.whilst(function() {
          return !connected;
        }, function(cb) {
          if (server.type === ServerType.MONGOCFG) {
            connected = true;
            return cb();
          }
          MongoClient.connect('mongodb://127.0.0.1:' + server.port + '/admin',
            function(err, db) {
              if (db) {
                db.close();
              }
              if (!err) {
                connected = true;
                console.log(server.port + ' started!');
                cb();
              } else {
                setTimeout(function() {
                  console.log('waiting..', err);
                  return cb();
                }, 1000);
              }
            });
        }, function(err) {
          cb();
        });
      }, cb);
    },
    function launchTwo(cb) {
      if (launchGroupTwo.length === 0) {
        return cb();
      }
      async.forEach(launchGroupTwo, function(server, cb) {
        server.start(cb);
      }, cb);
    },
    function waitForServers2(cb) {
      if (launchGroupTwo.length === 0) {
        return cb();
      }
      async.forEach(launchGroupTwo, function(server, cb) {
        var connected = false;
        async.whilst(function() {
          return !connected;
        }, function(cb) {
          if (server.type === ServerType.MONGOCFG) {
            connected = true;
            return cb();
          }
          MongoClient.connect('mongodb://127.0.0.1:' + server.port + '/admin',
            function(err, db) {
              if (db) {
                db.close();
              }
              if (!err) {
                connected = true;
                console.log(server.port + ' started!');
                cb();
              } else {
                setTimeout(function() {
                  console.log('waiting..');
                  return cb();
                }, 1000);
              }
            });
        }, function(err) {
          cb();
        });
      }, cb);
    },
    function configureServers(cb) {
      async.forEach(Object.keys(replicaSets), function(replSetName, cb) {
        var memberPort = replicaSets[replSetName].members[0];
        MongoClient.connect('mongodb://127.0.0.1:' + memberPort + '/test',
          function(err, db) {
            if (err) { return cb(err); }
            var setId = 0;
            db.executeDbAdminCommand({
              replSetInitiate: {
                _id: replSetName,
                members: replicaSets[replSetName].members.map(function(memberPort) {
                  return {
                    _id: setId++,
                    host: '127.0.0.1:' + memberPort
                  };
                })
              }
            }, function(err, result) {
              console.log('repl set initiate',
                result.documents[result.numberReturned-1]);
              if (db) {
                db.close();
              }
              cb(err);
            });
          });
      }, cb);
    },
    function waitForReplServers(cb) {
      if (!options.replicated) { return cb(); }
      console.log('waiting for repl sets to come online..');
      async.forEach(Object.keys(replicaSets), function(replSetName, cb) {
        var replActive = false;

        async.whilst(function() {
          return !replActive;
        }, function(cb) {
          var mongoServer = mongodb.Server('127.0.0.1',
            replicaSets[replSetName].members[0], {});
          var mongoClient = new mongodb.Db('test',
            mongoServer, {safe: true});
          mongoClient.open(function(err, db) {
            if (err || !db) {
              setTimeout(function() {
                return cb();
              }, 1000);
              return;
            }
            db.executeDbAdminCommand({
              replSetGetStatus: 1
            }, function(err, result) {
              if (db) { db.close(); }
              if (err) {
                setTimeout(function() {
                  return cb();
                }, 1000);
                return;
              }
              var replStatus = result.documents[result.numberReturned-1];
              var masterFound = false;
              (replStatus.members || []).forEach(function(member) {
                if (member.stateStr === 'PRIMARY') {
                  masterFound = true;
                }
              });
              if (masterFound) {
                replActive = true;
                return cb();
              } else {
                setTimeout(function() {
                  return cb();
                }, 1000);
              }
            });
          });
        }, function(err) {
          cb();
        });
      }, cb);
    },
    function configureShards(cb) {
      if (!options.sharded) { return cb(); }
      var shards = [];
      if (options.replicated) {
        // add each repl set as a shard
        Object.keys(replicaSets).forEach(function(replSetName) {
          shards.push(replSetName +
            '/127.0.0.1:' +
            replicaSets[replSetName].members[0]);
        });
      } else {
        // each mongod process is a shard
        servers.forEach(function(server) {
          if (server.type === ServerType.MONGOD) {
            shards.push('127.0.0.1:' + server.port);
          }
        });
      }
      MongoClient.connect('mongodb://127.0.0.1:' + startPort + '/admin',
        function(err, db) {
          if (err) { return cb(err); }
          var shardNum = 0;
          async.eachSeries(shards, function(shard, cb) {
            db.executeDbAdminCommand({
              addShard: shard,
              name: options.replicated ? undefined : shardNum++
            }, function(err, result) {
              console.log('addShard ran', result.documents[result.numberReturned-1]);
              cb(err);
            });
          }, cb);
        });
    }
  ], function(err) {
    if (!err) {
      console.log('cluster online.  mongo 127.0.0.1:' + startPort + ' to connect');
    } else {
      console.log('cluster startup error', err);
    }
  });

}

function main() {
  if (!checkRequirements()) {
    console.log('cannot continue, requirements not met.');
    return;
  }
  var options = processOptions(require('optimist').argv);
  start(options);
}


if (!module.parent) {
  main();
}
