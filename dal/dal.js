/**
 * Created by user on 7/7/15.
 */


/**
 *
 *
 *
 *CRUD items on any node
 * sync across AS
 * forward CRUD ... check if neighbors are not linked
 */

/*
 what are versions?
 last time the database or repo was updated
 we sync by comparing versions on a connection
 node a version for b is 0, while b is 50, so b will send all data between 0, and 50
 version in db is based on updated time ... so any record modified is sent

 i do not know if we need c, u d methods, they can call sync and send sync requests
 */

var sh = require('shelpers').shelpers;
var shelpers = require('shelpers');
var SequelizeHelper = shelpers.SequelizeHelper;
var EasyRemoteTester = shelpers.EasyRemoteTester;



function BasicClass() {
    var p = BasicClass.prototype;
    p = this;
    var self = this;
    p.method1 = function method1(url, appCode) {
    }

    p.proc = function debugLogger() {
        if ( self.silent == true) {
            return
        }
        sh.sLog(arguments)
    }
}

exports.BasicClass = BasicClass;





function MySQLDataRepo() {
    var p = MySQLDataRepo.prototype;
    p = this;
    var self = this;
    p.init = function init(url, fx) {
        self.version = 0;
        self.checkSources(function(){
            self.ready = true;
            sh.callIfDefined(fx)
        });
        self.settings = sh.dv(url, {});
        self.tableName = self.name + '_' + 'table';
    }

    p.getVersion = function getVersion( ) {
        return self.version;
    }
    /** Get proper version from database
     * Default is global_updated_at time of current record
     * @param fx
     */
    p.setVersion = function setVersion(fx ) {
        self.Table.findAll({ limit: 1, order: 'global_updated_at DESC' }).then(function onResults(objs) {
            if ( objs.length == 0 ) {
                self.version = 0;
                return;
            }
            self.proc('current version', objs[0].name, objs[0].global_updated_at.getTime())
            self.version = objs[0].global_updated_at.getTime();
            sh.callIfDefined(fx);
        })
    }

    function defineDbHelpers() {
        var dbHelper = {};
        self.dbHelper2 = dbHelper;
        dbHelper.count = function (fx, table, name) {
            table = sh.dv(table, self.Table);
            //console.error('count', table.name, name)
            table.count({where:{}}).then(function onResults(count) {
                self.count = count;
                self.proc('count', count)
                sh.callIfDefined(fx, count)
                //  self.version = objs.updated_at.getTime();
            })
        }

        dbHelper.countAll = function (fx, query) {
            query = sh.dv(query, {});
            self.Table.count({where:query}).then(function onResults(count) {
                self.count = count;
                self.proc('count', count)
                sh.callIfDefined(fx, count)
                //  self.version = objs.updated_at.getTime();
            })
        }

        dbHelper.getUntilDone = function (query, limit, fx, fxDone, count) {
            var index = 0;

            if ( count == null ) {

                dbHelper.countAll(function(initCount){
                    count = initCount;
                    nextQuery();
                }, query)
                return
            }

            function nextQuery(initCount) {
                self.proc( index, count, (index/count).toFixed(2));
                if ( index >= count ) {
                    sh.callIfDefined(fxDone)
                    sh.callIfDefined(fx, [], true)
                    return;
                };

                self.Table.findAll(
                    {
                        limit: limit,
                        offset: index,
                        where:query
                    }
                ).then(function onResults(objs) {
                        var records = [];
                        var ids= [];
                        sh.each(objs, function col(i,obj) {
                            records.push(obj.dataValues);
                            ids.push(obj.dataValues.id);
                        });
                        self.proc('sending', records.length, ids)
                        index += limit;
                        // var lastPage = records.length < limit;
                        //lastPage = index >= count;
                        // self.proc('...', lastPage, index, count)
                        sh.callIfDefined(fx, records, false)
                        sh.callIfDefined(nextQuery)
                    }
                ).catch(function (err) {
                        console.error(err, err.stack);
                        throw(err);
                    })
            }
            nextQuery();


        }


        dbHelper.search = function search (query,fx, limit, offset    ) {

            self.Table.findAll(
                {
                    limit: limit,
                    offset: offset,
                    where:query
                }
            ).then(function onResults(objs) {
                    var records = [];
                    var ids= [];
                    sh.each(objs, function col(i,obj) {
                        records.push(obj.dataValues);
                        ids.push(obj.dataValues.id);
                    });
                    sh.callIfDefined(fx, records)
                }
            ).catch(function (err) {
                    console.error(err, err.stack);
                    fx(err)
                    throw(err);
                })
        }

    }

    defineDbHelpers();


    p.getRecords = function getRecords(toVersion, fxGotRecords ) {


        self.proc('asked to send records')
        self.setVersion(function startSendingRecords() {
            self.dbHelper2.getUntilDone({}, 2,
                function returnRecords(objs, lastPage){
                    self.proc('sending records', objs.length)
                    fxGotRecords(objs, self.version, lastPage)
                })
        })


        return;
        self.count =
            fxGotRecords([], self.version);
        return 0;
    }

    p.create = function getVersion( records, fx ) {
        throw( new Error('deprec') );
        //return 0;

        sh.callIfDefined(fx)
    }

    p.update = function getVersion( records, fx ) {
        throw( new Error('deprec') );
        return 0;
    }

    p.fxDelete = function fxDelete( records, fx) {
        throw( new Error('deprec') );
        return 0;
    };

    p.destroyAllRecords = function (confirmed, fx) {
        if ( confirmed != true ) {
            return false;
        }

        self.Table.destroy({where:{}}).then(function() {
            sh.callIfDefined(fx);
            self.proc('all records destroyed')
        })

    }

    p.upsert = function upsert( records, fx) {
        records = sh.forceArray(records);
        var dict = {};
        var dictOfExistingItems = dict;
        var queryInner = {};
        var statements = [];

        var newRecords = [];
        var ids = [];
        sh.each(records, function putInDict(i,record){
                ids.push(record.id)
            }
        )
        self.proc(self.name, ':','upsert', records.length, ids)
        if ( records.length == 0 ) {
            sh.callIfDefined(fx);
            return;
        }



        sh.each(records, function putInDict(i,record){
            if ( record.id_timestamp == null || record.source_node == null ) {
                newRecords.push(record);
                record.id_timestamp = new Date();
                record.source_node = self.name;
                record.global_updated_at = new Date();
                //delete records[id]
                record.id = null;
                return;
            }
            if ( sh.isString(record.id_timestamp)) {
                record.id_timestamp = new Date(record.id_timestamp);
            }
            if ( sh.isString(record.global_updated_at)) {
                record.global_updated_at = new Date(record.global_updated_at);
            }

            dict[record.id_timestamp.getTime()+record.source_node] = record;
            /*statements.push(SequelizeHelper.Sequlize.AND(


             ))*/

            statements.push({
                id_timestamp:record.id_timestamp,
                source_node:record.source_node
            });
        })

        if ( statements.length > 0 ) {
            queryInner = SequelizeHelper.Sequelize.or(statements)
            queryInner = SequelizeHelper.Sequelize.or.apply(this, statements)

            //find all matching records
            var query = {where: queryInner};

            self.Table.findAll(query).then(function (results) {
                self.proc('found existing records');
                sh.each(results, function (i, eRecord ) {
                    var eRecordId = eRecord.id_timestamp.getTime()+eRecord.source_node;
                    var match = dictOfExistingItems[eRecordId];
                    if ( match == null ) {
                        self.proc('warning', 'look for record did not have in database')
                        //newRecords.push()
                        return;
                    }

                    var match=  eRecord.dataValues.global_updated_at.toString() == match.global_updated_at.toString()
                    self.proc('compare',
                        eRecord.name,
                        match,
                        eRecord.dataValues.global_updated_at, match.global_updated_at);

                    if (match) {
                        self.proc('warning', 'rec\'v object that is already up to date', eRecord.dataValues)
                    } else {
                        eRecord.updateAttributes(match);
                    }
                    //handled item
                    dictOfExistingItems[eRecordId] = null;
                });
                createNewRecords();
            });
        } else {
            createNewRecords();
        }

        //update them all

        //add the rest
        function createNewRecords() {
            //mixin un copied records
            sh.each(dictOfExistingItems, function addToNewRecords(i, eRecord ) {
                if ( eRecord == null ) {
                    //already updated
                    return;
                }
                console.error('removing id on', eRecord.id)
                eRecord.id = null;
                newRecords.push(eRecord);
            });

            if ( newRecords.length > 0 ) {
                self.Table.bulkCreate(newRecords).then(function (objs) {

                    self.proc('all records created', objs.length);
                    //sh.each(objs, function (i, eRecord) {
                    // var match = dict[eRecord.id_timestamp.toString() + eRecord.source]
                    // eRecord.updateAttributes(match)
                    // })
                    sh.callIfDefined(fx2);

                }).catch(function (err) {
                    console.error(err, err.stack)
                    throw  err
                })
            } else {
                self.proc('no records to create')
                sh.callIfDefined(fx2)
            }



            function fx2() {
                self.setVersion(fx);
            }
        }

    }
    /**
     * time_rand_id <-- this becomes new version
     * source
     * version
     * deleted
     *
     */
    p.checkSources = function (fx) {
        var dbHelper = {};
        self.dbHelper = dbHelper;
        dbHelper.createDatabase = function () {
            var mysql      = require('mysql');
            var connection = mysql.createConnection({
                host     : 'localhost',
                user     : 'root',
                password : 'password'
            });

            connection.connect(function(err) {
                if (err) {
                    console.error('error connecting: ' + err.stack);
                    return;
                }

                console.log('connected as id ' + connection.threadId);


                connection.query("CREATE DATABASE  IF NOT EXISTS test_sync;", function(err, rows) {
                    // connected! (unless `err` is set)
                    dbHelper.createTable()
                });
            });
        }

        dbHelper.createTable = function (sttgs) {
            sttgs = sh.dv(sttgs, {})
            if ( self.settings.mysql == null ) {
                var db = {}
                db.database = 'test_sync';
                db.user = 'root';
                db.password = 'password';
                db.port = '3306';
                // db.logging = false;
                self.settings.mysql = db;
            }
            if ( dbHelper.sequelize == null ) {
                var mysqlSettings = sh.clone(self.settings.mysql);
                mysqlSettings.cb = finishedInitMySQL;
                var sequelize = SequelizeHelper.createSQL(mysqlSettings);
            } else {
                var sequelize = dbHelper.sequelize
                finishedInitMySQL(sequelize);
            }
            function finishedInitMySQL(sequelize){
                self.sequelize = sequelize;

                var tableSettings = {};
                if ( dbHelper.noSync == true ) {
                    tableSettings.force = false
                    tableSettings.sync = false;
                }
                tableSettings.name = self.tableName
                tableSettings.name = sh.dv(sttgs.name, tableSettings.name);
                tableSettings.createFields =  {name: "", desc: "", user_id: 0,
                    imdb_id: "", content_id: 0,
                    progress:0}
                var reqD =  {source_node:"", id_timestamp:new Date(),
                    global_updated_at:new Date(), //make another field that must be changed
                    version:0, deleted:true}
                //global_updated_at
                //time_id
                sh.mergeObjects(reqD, tableSettings.createFields);
                tableSettings.sequelize = sequelize;
                SequelizeHelper.defineTable(tableSettings, doneWithTable);
            }

            function doneWithTable(table) {
                console.log('table ready')
                if ( sttgs.storeTable != false ) {
                    self.Table = table;
                    self.setVersion();
                }
                sh.callIfDefined(fx);
                sh.callIfDefined(sttgs.fx, table)
            }
        }
        //TODO: rip from settings using mysql
        dbHelper.getExistingTable = function (name, fx) {
            var settings = {};
            settings.name = name;
            settings.fx = fx;
            settings.storeTable = false;
            dbHelper.createTable(settings);
        }
        //test
        //create database
        dbHelper.createDatabase()
        //create table

        //check the table



        //need col
    }


    function defineTests() {
        p.compareTables = function compareTables(nameA, nameB, throwError, fx) {

            var t = EasyRemoteTester.create('test compareTables API',{});
            var data = {};

            if ( MySQLDataRepo.dict != null ) {
                MySQLDataRepo.dict= {};
            }

            self.dbHelper.noSync = true;
            self.dbHelper.sequelize = self.sequelize;

            t.add(function getTable1() {
                self.dbHelper.getExistingTable(nameA,
                    function (tbl) {
                        data.table1 = tbl;
                        console.error('got back', tbl.name, nameA)
                        // MySQLDataRepo.dict[] =
                        t.cb();
                    }
                )
            });
            t.add(function getTable2() {
                self.dbHelper.getExistingTable(nameB,
                    function (tbl) {
                        data.table2 = tbl;
                        console.error('got back', tbl.name, nameB)
                        t.cb();
                    }
                )
            });
            t.add(function countTable1() {
                self.dbHelper2.count(function (count) {
                    data.count1 = count;
                    t.cb();
                }, data.table1, data.table1.name)
            });

            t.add(function countTable2() {
                self.dbHelper2.count(function (count) {
                    data.count2 = count;
                    t.cb();
                }, data.table2, data.table2.name)
            })

            t.add(function compareSize() {
                // (data.count1 == data.count2, 'table not same size' )

                console.log(nameA,data.count1,
                    nameB, data.count2, data.count1 == data.count2 );

                if ( throwError != false )
                    t.assert(data.count1 == data.count2,'table not same size' );

                sh.callIfDefined(fx);
                t.cb();
            })

        }
    }

    defineTests();

    p.proc = function debugLogger() {
        if ( self.silent == true) {
            return
        }
        arguments = sh.convertArgumentsToArray(arguments)
        arguments.unshift(self.name+'-repo:')
        return sh.sLog(arguments)
    }
}

exports.MySQLDataRepo = MySQLDataRepo;


function Actor() {
    var p = Actor.prototype;
    p = this;
    var self = this;

    var types = {};
    types.SyncVersion_AreYouCurrentWithMe = 'SyncVersion_AreYouCurrentWithMe';
    types.SyncGet_Records = 'SyncGet_Records'; //ask peer to send back records
    types.RequestSync = 'RequestSync';//tell peers to make sync request
    types.SyncData_SendComplete = 'SyncData_SendComplete';//tell peers to make sync request
    types.SyncData = 'SyncData'; //Data results coming back
    types.CrudCreate = 'CrudCreate';
    types.CrudUpdate = 'CrudUpdate =';
    types.CrudDelete = 'CrudDelete';

    p.init = function method1(url, appCode) {
        self.linksTo = [];
        self.peers = self.linksTo;
        url = sh.dv(url, {});
        self.settings = url;
        if ( self.settings.name == null ) {
            self.settings.name = self.name;
        }

        self.settings.fileSettings = self.name+'_dal_node.json'
        self.settings.fileLog = self.name+'_dal_node_log.txt'

        self.data = {};
        self.data.counts = {};
        self.data.counts.recordsSent = 0;
        self.data.counts.recordsReceived = 0;
        self.data.counts.syncAttempts = 0;
        self.data.timeStart = new Date();


        self.data.converged = false

        self.actorLog = []; //important events only ...

        self.data.port;

        self.dictPeers = {};

        self.types = {};
        self.types = types;

        self.repo = new MySQLDataRepo();
        self.repo.name = self.name;
        self.repo.init(null,  self.fxStartInit);

        self.settings.peers= [];
    }

    p.linkTo = function linkTo(peer) {
        self.linksTo.push(peer)
        self.dictPeers[peer.name]=peer;
        self.settings.peers.push(peer.name);
    }

    function defineUtils() {
        p.utils = {};
        /**
         * I need to forward a crud request to a pper
         * but i do not want to duplicate them
         * so go through my peers, if any of my peers, links to this node, then do not forward it
         *
         * IE, b links to a, c and d
         * requests from c, should go to a,
         * requests from a should go to c and d
         * @param x
         */
        p.utils.forwardRequestsTo = function (nodeFrom) {
            var sendTo = [];
            sh.each(self.peers, function (i, peer) {
                if (peer == nodeFrom) {
                    return;
                }
                if (peer.settings.peers.indexOf(nodeFrom.name)) {
                    return;
                }
                sendTo.push(peer);
            });

            return sendTo;
        }

        p.log = function log(msg) {
            self.actorLog.push(msg);
            if ( self.globalLog != null ) {
                self.globalLog.push(sh.getTimeStamp()+':' + msg);
            }
        }

        p.saveLog = function saveLog() {
            var strFile = each.print(self.actorLog)
            sh.writeFile(self.fileLog, strFile);
        }

        /**
         * Check all peers, are we converged?
         */
        p.utils.convergenceTest = function convergenceTest() {

            self.data.converged = false;
            var unconvergedPeers = [];
            sh.each(self.peers, function (i, peer) {
                if (peer.my_version != peer.version ) {
                    unconvergedPeers.push(peer);
                    return false;
                }
                if (peer.my_version != self.repo.getVersion() ) {
                    unconvergedPeers.push(peer);
                    return false;
                }
            });

            if ( unconvergedPeers.length == 0 ) {
                self.data.converged = true;
                self.log('converged')
            } else {
                self.data.converged = false;
            }
        }
    }
    defineUtils();

    function defineConnection() {
        p.connectToOthers = function connectToOthers(url, appCode) {
            var sockets = [];
            self.sockets = sockets;
            sh.each( self.linksTo, function (i,peer) {
                var url = 'http://localhost'+':'+ peer.settings.port;
                console.log('trying to reach', peer.name, url)
                var socket = require('socket.io-client')(url);

                var methodInterceptor = function methodInterceptor() {

                }
                var fxOn_Orig = socket.on;
                socket.on = function onSocketSecurityHelper (type, fx ) {
                    //asdf.g
                    fxOn_Orig(type, function securityInterceptor(){
                        arguments = sh.convertArgumentsToArray(arguments);
                        if ( self.settings.password ) {
                            if ( data.password != self.settings.password ) {
                                console.error('no auth')
                                return ;
                            }
                        }
                        fx.apply(socket, arguments);
                    });
                };
                var fxEmit = socket.emit;
                socket.emit = function emitSocketSecurityHelper (  ) {
                    arguments = sh.convertArgumentsToArray(arguments);
                    var data = arguments[0];

                    if ( self.settings.password ) {
                        data.password = self.settings.password
                    }

                    fxEmit.apply(socket, arguments);
                }

                socket.on('connect', function(){
                    console.log('connected', peer.name, self.name )
                    self.con.sync()
                });
                socket.on('event', function(data){});
                socket.on('disconnect', function(){});
                sockets.push(socket);
                peer.data.url= url;
                peer.socket = socket;
                //TODO: get stored version from db
            });


            self.connected = true
            self.fxStartInit();

        }


        p.fxStartInit = function () {

            if ( self.repo.ready != true ||
                self.connected != true
            ) {
                return
            }
            if ( self.ranInit == true ) {
                return
            }
            self.ranInit = true;
            setTimeout(function () {
                if ( self.fxStart != null )
                    self.fxStart();

            }, 500)
        }

        p.start = function start( ) {
            // self.linksTo.push(url)

            // Setup basic express server
            var express = require('express');
            var app = express();
            var server = require('http').createServer(app);
            var io = require('socket.io')(server);
            var port = process.env.PORT || 3000;
            self.defineRoutes(app)
            port = sh.dv(  self.settings.port, port);
            console.log(self.name, port );
            server.listen(port, function () {
                console.log('Server listening at port %d', port, self.name);
                self.connectToOthers();
            });
            // Routing
            app.use(express.static(__dirname + '/public'));
            // Chatroom
            // usernames which are currently connected to the chat
            var usernames = {};
            var numUsers = 0;
            io.on('connection', function (socket) {
                self.socket = socket;
                self.con.defineCrud();
                self.con.defineCrud_Sync();
                self.con.defineSync();
                var addedUser = false;
                // when the client emits 'new message', this listens and executes
                socket.on('new message', function (data) {
                    // we tell the client to execute 'new message'
                    socket.broadcast.emit('new message', {
                        username: socket.username,
                        message: data
                    });
                });
                // when the client emits 'add user', this listens and executes
                socket.on('add user', function (username) {
                    // we store the username in the socket session for this client
                    socket.username = username;
                    // add the client's username to the global list
                    usernames[username] = username;
                    ++numUsers;
                    addedUser = true;
                    socket.emit('login', {
                        numUsers: numUsers
                    });
                    // echo globally (all clients) that a person has connected
                    socket.broadcast.emit('user joined', {
                        username: socket.username,
                        numUsers: numUsers
                    });
                });
                // when the client emits 'typing', we broadcast it to others
                socket.on('typing', function () {
                    socket.broadcast.emit('typing', {
                        username: socket.username
                    });
                });



                // when the client emits 'stop typing', we broadcast it to others
                socket.on('stop typing', function () {
                    socket.broadcast.emit('stop typing', {
                        username: socket.username
                    });
                });
                // when the user disconnects.. perform this
                socket.on('disconnect', function () {
                    // remove the username from global usernames list
                    if (addedUser) {
                        delete usernames[socket.username];
                        --numUsers;
                        // echo globally that this client has left
                        socket.broadcast.emit('user left', {
                            username: socket.username,
                            numUsers: numUsers
                        });
                    }
                });
            });

        }
    }
    defineConnection();

    function defineProcesses() {

        p.con = {};
        p.connection = p.con;

        p.connection.startConnection = function startconnect() {
            // self.sycned=false;
        }

        /**
         * Tell peers to get my newest updates if they are out of
         * date with me.
         */
        p.con.sync = function sync_AskPeers(onlyThisPeer) {
            self.settings.version = self.repo.getVersion();

            var peers = self.peers;
            if ( onlyThisPeer != null ) {
                peers = [onlyThisPeer];
            }
            sh.each( peers, function askPeerToSync(i,peer) {
                self.proc('sync peering', peer.name )
                var my_version = self.settings.version
                /*if ( peer.my_version == null ){
                 peer.my_version = -1;//force initial sync
                 }*/
                if ( peer.my_version !=  self.settings.version ){
                    peer.my_version = -1;//force initial sync
                    my_version = -1;
                }
                peer.socket.emit(types.SyncVersion_AreYouCurrentWithMe, {
                    version:peer.version,
                    // my_version:  self.settings.version my_version
                    //why do you keep setting this to my_version?
                    //this only works on end nodes ...
                    //each peer connection needs to have a sync number
                    my_version:my_version,
                    name:self.name,
                    data:sh.clone(self.settings)
                });
            });
        };


        /**
         * Confusing Grammar leads to confusion
         * This method is more practical
         *
         * Send my version all data to peers
         * send up to
         */
        p.con.sync_Forward = function () {
            //self.settings.version = self.repo.getVersion();
            sh.each( self.peers, function (i,peer) {
                //Translation: I am at version X for your client.x-x--xPlease send any updates
                //Check your version for me ... if there is  not a match ... request
                //an update
                peer.socket.emit(types.SyncVersion_AmICurrentWithYou, {
                    version:peer.version,
                    name:self.name,
                    data:sh.clone(self.settings)
                });
            });
        };

        p.con.requestSync = function () {

            self.repo.setVersion(requestSyncAction)

            function requestSyncAction() {
                self.settings.version = self.repo.getVersion();
                sh.each( self.peers, function (i,peer) {
                    self.proc(sh.q(self.name), 'asking', sh.q(peer.name), 'to sync')
                    peer.socket.emit(types.RequestSync, {
                        version:peer.version,
                        name:self.name,
                        data:sh.clone(self.settings)
                    });
                });
            }

        };

        p.con.defineSync = function () {
            //Translation: This peer is at version X, is he current?
            //if not, tell him to sync to me
            self.socket.on(types.SyncVersion_AreYouCurrentWithMe, function areWeInSync(data) {
                var peer = self.dictPeers[data.data.name];
                self.data.counts.syncAttempts++
                var peers_version = data.my_version; //data.data.version;
                var peersVersion_vFrom = peers_version;

                var vMyVersionOfDbForPeer = peer.version;
                if ( peer.version == null ) {
                    vMyVersionOfDbForPeer = 0;
                }

                var msg = self.proc(types.SyncVersion_AreYouCurrentWithMe,
                    sh.q( data.name), 'has asked', 'me',
                    sh.paren(sh.q(self.name)), 'to sync with him',
                    'i my connection is at at version', vMyVersionOfDbForPeer,
                    'he is at version', peersVersion_vFrom
                );
                self.log(msg);

                //peersVersion_vFrom > vMyVersionOfDbForPeer
                //new peer may have records on odd versions ... if there is no match, then sync?
                if ( peersVersion_vFrom !=  vMyVersionOfDbForPeer ) {
                    self.proc('need to update', sh.q(peer.name), 'is at ',
                        peersVersion_vFrom, 'I am at',
                        vMyVersionOfDbForPeer)
                    //translation: I need to update, please give me all data between versions
                    peer.socket.emit(types.SyncGet_Records, {
                        from: vMyVersionOfDbForPeer,
                        to: peersVersion_vFrom,
                        name: self.name,
                        data: sh.clone(self.settings)
                    });
                } else {
                    self.proc('no sync necessary');
                    self.utils.convergenceTest();
                }
                /*else if ( peersVersion_vFrom < vMyVersionOfDbForPeer ) {
                 self.proc('need to Reverse -- update', sh.q(peer.name), 'is at ',
                 peersVersion_vFrom, 'I am at',
                 vMyVersionOfDbForPeer)
                 self.con.sync(peer)
                 } else {
                 self.proc('no need to sync', sh.q(peer.name), 'is at ',
                 peersVersion_vFrom, 'I am at',
                 vMyVersionOfDbForPeer)
                 }*/
            });

            /*
             //Translation: This peer is at version X, is he current?
             //if not, tell him to sync to me
             self.socket.on(types.SyncVersion_AreYouCurrentWithMe, function (data) {
             var peer = self.dictPeers[data.data.name];

             var vFrom = data.version;
             var vFrom_PeersVersion = vFrom;

             var vTo = peer.version; //self.settings.version;
             vTo = self.repo.getVersion();
             if ( vTo == null ) {
             vTo = 0
             }
             var vMyVersionOfDb = vTo;




             //  var vTo = data.data.version;
             // var vFrom = self.settings.version;
             self.proc(types.SyncVersion_AmICurrentWithYou, 'sync test', self.name,
             vMyVersionOfDb ,'peers version is ', data.name, vFrom_PeersVersion);

             if ( vFrom_PeersVersion < vMyVersionOfDb ) {
             self.proc('need to update', sh.q(peer.name), 'is at ', vFrom_PeersVersion, 'I am at', vMyVersionOfDb)
             //translation: I need to update, please give me all data between versions
             peer.socket.emit(types.SyncGet_Records, {
             from:vFrom,
             to:vTo,
             name:self.name,
             data:sh.clone(self.settings)
             });
             }
             });

             */




            //TODO: should we sync all peers? ... no b/c if new node comes up it needs what
            //it needs
            //handle sync requests
            //translation: this peer needs some data to update, send back records
            self.socket.on(types.SyncGet_Records, function onSync_Get (data) {
                var peer = self.dictPeers[data.data.name];

                self.data.convergedAt = new Date();
                var vFrom = data.data.version;
                var vTo = self.settings.version;
                console.log('Got sync request from ', sh.q(peer.name), '---', types.SyncGet_Records, sh.q(self.name),
                    self.settings.version ,'from', data.name, data.data.version);

                //self.proc(self.name, 'need to get', vFrom)

                if (vFrom == vTo) {
                    self.proc('skipping sync', 'versions are the same')
                    return;
                }

                //callback will update records
                //var records =

                self.repo.getRecords(vTo,
                    function onSendRecords(records, version, lastPage) {

                        self.proc(sh.q(self.name)+':', 'sending ', version, 'to', sh.q(peer.name), records.length)
                        peer.socket.emit(types.SyncData, {
                            from:vFrom,
                            to:version,
                            name:self.name,
                            data:sh.clone(self.settings),
                            newVersion:version,
                            records:records //TODO: enforce maximum size
                        });
                        self.data.counts.recordsSent++;
                        //if sync over ...
                        if ( lastPage ){
                            peer.socket.emit(types.SyncData_SendComplete, {
                                from:vFrom,
                                to:version,
                                name:self.name,
                                data:sh.clone(self.settings),
                                newVersion:version,
                            });
                            //
                            // self.repo.setVersion(postSyncSync_IHaveJustSendAndUpdateOut_DoesAnyoneElseNeedAnUpdate)
                            self.proc('done syncing');

                            function postSyncSync_IHaveJustSendAndUpdateOut_DoesAnyoneElseNeedAnUpdate() {
                                //do not version here .. this was a send...
                                //bk:peerversioning
                                //peer.version = self.repo.getVersion();
                                setTimeout(function(){

                                    //self.con.requestSync();
                                    self.con.sync();
                                }, 1000);
                            };

                        }
                    });
                //if ( vFrom > vTo ) {
                //}
            });

            self.socket.on(types.SyncData, function onRecieveData (data) {
                var peer = self.dictPeers[data.data.name];
                var vTo = data.data.version;
                var vFrom = peer.version;
                console.log(  types.SyncData, self.name,
                    self.settings.version ,'from', data.name,vFrom, '-->', vTo);
                self.proc(types.SyncData, 'connection', sh.paren(sh.q(self.name)+','+sh.q(peer.name))
                    , 'up to version', vTo);

                sh.each(data.records, function goThroughRec(i,record) {
                    console.error(peer.name+'-->'+self.name+':',record.id, record.source_node, record.name )
                })

                self.data.counts.recordsReceived++;
                self.repo.upsert(data.records);
                peer.version = vTo;


            });

            self.socket.on(types.SyncData_SendComplete, function (data) {
                var peer = self.dictPeers[data.data.name];
                var vTo = data.data.version;
                var vFrom = peer.version;
                //console.log(  types.SyncData_SendComplete, self.name,
                //    self.settings.version ,'from', data.name,vFrom, '-->', vTo);
                var msg = self.proc(types.SyncData_SendComplete,'brought', 'connection', sh.paren(sh.q(self.name)+','+sh.q(peer.name))
                    , 'up to version', vTo);

                peer.version = vTo;

                //var otherPeers = self.utils.forwardRequestsTo(peer)
                //no need toworry about peer as # shuld be thesame
                //self.con.requestSync();
                //tell my peers to sync to me
                self.repo.setVersion(function setVersionAfterSync(){
                    peer.my_version = self.repo.getVersion();
                    self.log(msg + ' i have x records');
                    self.con.sync();
                    setTimeout(function onUpdate() {
                        self.con.requestSync();
                    },2000)

                })


            });

            self.socket.on(types.RequestSync, function (data) {
                self.con.sync();
            });
        }




        p.con.syncDance = function () {

            var dance = new Dance();
            from
            to

            /*
             tell b we are dancing
             lock
             send count my records
             send 1 and 5 record to b,
             b will confirm it recieved to (should it send it's own updates?) ... no for simplicty
             send 5-10 .. until all done

             flaw: what if records inbetween were modified

             2  - simpliest cast
             send first 5 record ids and updated time to b.
             if b has an issue, it will send those 5 records to B
             does offset have to match?

             how to implement:
             modify sendrecords to , sendrecords_verify
             sendrecords_verify only sends updated dates and sources
             b recieves sendrecords_verify pulls query for dates, and matches them, if there is an error send
             get full set,
             if OK;
             b sends - get record after X -- this means no dancing
             a, if no more , just send sync complete

             if Bad:
             b sends sendrecord_verify_getfullset
             a rec sr_v_gfs and sends back full records
             */
        }

        /**
         * C R U D L
         * Create method stubs for each action
         * update the version after each update, forward processing to repo
         *
         * method to send creation
         * method to handle creation
         */
        p.connection.defineCrud = function defineCrud() {

            p.connection.create = function create(records, version) {
                version = sh.dv(version, self.repo.getVersion());
                self.settings.version = version;
                sh.each( self.peers, function (i,peer) {
                    peer.socket.emit(types.CrudCreate, {
                        version:self.settings.version,
                        name:self.name,
                        records:records,
                        data:sh.clone(self.settings)
                    });
                });
            }

            self.socket.on(types.CrudCreate, function (data) {
                var peer = self.dictPeers[data.data.name];
                var vTo = data.data.version;
                self.proc(self.name, types.CrudCreate, vTo)
                //callback will update records
                self.repo.create(data.records)
            });

            p.connection.update = function update(records, version) {
                version = sh.dv(version, self.repo.getVersion());
                self.settings.version = version;
                sh.each( self.peers, function (i,peer) {
                    peer.socket.emit(types.CrudUpdate, {
                        version:self.settings.version,
                        name:self.name,
                        records:records,
                        data:sh.clone(self.settings)
                    });
                });
            }

            self.socket.on(types.CrudUpdate, function (data) {
                var peer = self.dictPeers[data.data.name];
                var vTo = data.data.version;
                self.proc(self.name, types.CrudUpdate, vTo)
                //callback will update records
                self.repo.update(data.records)
            });




            p.connection.delete = function fxDelete(records, version) {
                version = sh.dv(version, self.repo.getVersion());
                self.settings.version = version;
                sh.each( self.peers, function (i,peer) {
                    peer.socket.emit(types.CrudDelete, {
                        version:self.settings.version,
                        name:self.name,
                        records:records,
                        data:sh.clone(self.settings)
                    });
                });
            }

            self.socket.on(types.CrudDelete, function (data) {
                var peer = self.dictPeers[data.data.name];
                var vTo = data.data.version;
                self.proc(self.name, types.CrudDelete, vTo)
                //callback will update records
                self.repo.fxDelete(data.records)
            });


        }

        /**
         * Override crud methods with methods that sync
         */
        p.connection.defineCrud_Sync = function defineCrud() {

            p.connection.create = function create(records, version) {
                self.log('creating records', records.length);
                self.repo.upsert(records, function recordsCreated() {
                    self.con.sync();
                })
                return;
                /*
                 version = sh.dv(version, self.repo.getVersion());
                 self.settings.version = version;
                 sh.each( self.peers, function (i,peer) {
                 peer.socket.emit(types.CrudCreate, {
                 version:self.settings.version,
                 name:self.name,
                 records:records,
                 data:sh.clone(self.settings)
                 });
                 });*/
            }

            self.socket.on(types.CrudCreate, function (data) {
                var peer = self.dictPeers[data.data.name];
                var vTo = data.data.version;
                self.proc(self.name, types.CrudCreate, vTo)
                //callback will update records
                self.repo.create(data.records)
            });

            p.connection.update = function update(records, version) {
                version = sh.dv(version, self.repo.getVersion());
                self.settings.version = version;
                sh.each( self.peers, function (i,peer) {
                    peer.socket.emit(types.CrudUpdate, {
                        version:self.settings.version,
                        name:self.name,
                        records:records,
                        data:sh.clone(self.settings)
                    });
                });
            }

            self.socket.on(types.CrudUpdate, function (data) {
                var peer = self.dictPeers[data.data.name];
                var vTo = data.data.version;
                self.proc(self.name, types.CrudUpdate, vTo)
                //callback will update records
                self.repo.update(data.records)
            });




            p.connection.delete = function fxDelete(records, version) {
                version = sh.dv(version, self.repo.getVersion());
                self.settings.version = version;
                sh.each( self.peers, function (i,peer) {
                    peer.socket.emit(types.CrudDelete, {
                        version:self.settings.version,
                        name:self.name,
                        records:records,
                        data:sh.clone(self.settings)
                    });
                });
            }

            self.socket.on(types.CrudDelete, function (data) {
                var peer = self.dictPeers[data.data.name];
                var vTo = data.data.version;
                self.proc(self.name, types.CrudDelete, vTo)
                //callback will update records
                self.repo.fxDelete(data.records)
            });


        }
        /**
         * TODO: implement smark check-sum feature
         */
        p.connection.verify = function defineVerify() {
            //divert to sync
            self.con.sync();
        }


    }

    defineProcesses();


    function defineSavingState() {


        p.saveState = function saveState() {
            /*
             peers
             */
            var save = {}
            save.settings = self.settings;
            var peersTemp = [];
            sh.each(self.peers, function onConnectPeer(i, peer) {
                var tempPeer = {}
                tempPeer.name = peer.name;
                tempPeer.settings = peer.settings;
                tempPeer.version = peer.version;
                peersTemp.push(tempPeer)
            });
            save.peers = peersTemp;
            save.version = self.version;

            var str = JSON.stringify(save);
            str = sh.toJSONString(save);
            sh.writeFile(self.settings.fileSettings, str);
        }

        p.loadState = function loaState() {
            var obj = sh.readFile(self.settings.fileSettings, '');
            obj = JSON.parse(obj);


            var peer = self.dictPeers[data.data.name];

            if (self.settings.reloadPeers != false) {
                self.peersX = obj.peers;
                //go thoguth peers, reconnect them up
                sh.each(self.peersX, function onConnectPeer(i, peer) {
                    self.linkTo(peer);
                });
            }
        }

        p.saveSettingsEveryXMinutes = function saveSettingsEveryXMinutes(min) {
            clearInterval(self.saveInterval);
            self.saveInterval = setInterval(self.saveState, 1000 * 60 * min)
        }


    }
    defineSavingState();


    self.defineRoutes = function defineRoutes(server) {
        server.get('/config', function (req, res) {
            res.end('....')
        });

        server.get('/counts', function (req, res) {
            self.repo.dbHelper2.count( function(count) {
                res.end(count.toString())
            });
        });

        server.get('/search', function (req, res) {
            self.repo.dbHelper2.count( function(count) {
                res.end(count)
            });
        });

        server.get('/converged', function (req, res) {
            res.end(self.data.converged.toString())
        });

        /*
         server.get('/config', function (req, res) {
         res.end('....')
         });
         */


    }

    p.proc = function debugLogger() {
        if ( self.silent == true) {
            return
        }
        arguments = sh.convertArgumentsToArray(arguments)
        arguments.unshift(self.name+'-Peer:')
        return sh.sLog(arguments)
    }
}

exports.Actor = Actor;


function TopologyHelper() {
    var p = TopologyHelper.prototype;
    p = this;
    var self = this;
    p.loadTop = function loadTop(topology, dictNodeSettings) {
        self.globalLog = [];
        var count =  0
        function createNewActor(name, settings) {
            var actor = new Actor()
            actor.name = name
            actor.init();
            actor.settings.port = count + 3000;
            actor.url = 'http://127.0.0.1:'+
                actor.settings.port+ '/';
            count++

            actor.globalLog = self.globalLog;
            return actor;
        }

        var dictNodes = {};
        var allNodes = [];
        self.dictNodes = dictNodes;

        sh.each(topology, function linkPairs(i,topSet) {

            var a = topSet[0];
            var b = topSet[1];

            if ( dictNodes[a]==null) {
                var nodeA = createNewActor(a)
                allNodes.push(nodeA)
                dictNodes[a] = nodeA;
            } else {
                nodeA =  dictNodes[a]
            }

            if ( sh.isArray(b) == false ) {
                b = [b];
            }

            sh.each(b, function createBNodes(i,b){
                if ( dictNodes[b]!=null) {
                    nodeB = dictNodes[b];
                } else {
                    var nodeB = createNewActor(b)
                    allNodes.push(nodeB)
                    dictNodes[b] = nodeB;
                }
                //make copies to simulate real modeling
                nodeA.linkTo(sh.clone(nodeB));
                nodeB.linkTo(sh.clone(nodeA));
            })

        })


        /* //full mesh
         sh.each(allNodes, function linkAllNodes(i,nodeFrom){
         nodeFrom.settings.port = 3000 + i;
         sh.each(allNodes, function linkAllNodes(i,nodeTo){
         if ( nodeFrom == nodeTo ) { return }
         nodeFrom.linkTo(nodeTo)
         nodeTo.linkTo(nodeFrom)
         })
         });
         */

        sh.each(dictNodeSettings, function addNode(i, nodeSettings) {
            var node = dictNodes[i];
            if ( nodeSettings.version != null )
                node.repo.version = nodeSettings.version;
        });


        sh.each(allNodes, function linkAllNodes(i,nodeFrom){
            nodeFrom.start();
        })


        return dictNodes;
    }


    p.collectHTTPRequest = function collectHTTPRequest(url, fx, display) {

        function AsyncRequest() {
            var p = AsyncRequest.prototype;
            p = this;
            var self = this;

            var request = require('request');

            p.iterate = function method1(settings) {
                self.settings = settings;

                self.results = [];
                self.dictResults = {};
                //could use async but need failure methods
                self.settings.itemsLength = self.settings.items.length;
                if ( self.settings.items.length == null ) {
                    self.settings.itemsLength = 0;
                    sh.each(self.settings.items, function countDict(i, item) {
                        self.settings.itemsLength++;
                    });
                }

                sh.each(self.settings.items, function runMethodOnNode(i, item) {

                    var name = item.name;
                    var reqOptions = {};
                    reqOptions.url = item.url + url

                    var cb_ = function callbackResults ( result ) {
                        self.dictResults[name] = result;
                        self.results.push(result)
                        if ( self.results.length == self.settings.itemsLength ) {
                            sh.callIfDefined(self.settings.fxEnd, self.results, self.dictResults)
                        }
                    }
                    request(reqOptions, function onResponse(error, response, body) {
                        if ( error != null ) {
                            cb_(error)
                            return;
                        }
                        if ( response.statusCode != 200) {
                            cb_( response.statusCode + ' '  + body)
                            return;
                        }
                        cb_(body)
                    })

                    if ( self.settings.timeout != null ) {
                        setTimeout(function finishEarlyIteration() {
                            cb_('timeout ' + reqOptions.url);
                            cb_ = function (err) { console.log('bad response '+ err)}
                        }, self.settings.timeout*1000)
                    }
                })
            };

            p.proc = function debugLogger() {
                if ( self.silent == true) {
                    return
                }
                sh.sLog(arguments)
            };
        }
        exports.AsyncRequest = AsyncRequest;


        var aR = new AsyncRequest()
        var settings = {};
        settings.name = 'go through all urls'
        settings.items = self.dictNodes;
        settings.timeout= 2;
        settings.url = 'config'
        settings.fxEnd = function (results , dict) {
            if ( display == true ) {
                console.log(settings.name);
                sh.each.print(results);
            }
            sh.callIfDefined(fx, results, dict)
        };
        /*settings.fxGenRoute = function (node ) {
         return node.url;
         }
         settings.fxGenName = function (item) {
         return item.name;
         }*/
        settings.nameProp = 'name'
        settings.propUrl = 'url'
        aR.iterate(settings)



    }



    p.writeLog = function writeLog(filename) {
        filename = sh.dv(filename, 'global_log.txt')
        var strGlobalLogContents = sh.each.print(self.globalLog)
        sh.writeFile(filename, strGlobalLogContents.join('\n'));
    }
    p.proc = function debugLogger() {
        if ( self.silent == true) {
            return
        }
        return sh.sLog(arguments)
    }
}

exports.TopologyHelper = TopologyHelper;

if (module.parent == null) {
    var netw = new TopologyHelper();
    'a --> b --> [c,d]'
    var top = [
        ['a', 'b'],
        ['b', ['c', 'd']],
        ['d', ['e']]
    ]
    var dict = {};
    dict['a'] = {};
    //dict['a'].version = 1;
    var dict2 = netw.loadTop(top, dict);

    var nodeA = dict2['a'];
    var nodeD = dict2['d'];
    //function createItems() {
    nodeA.fxStart = function () {
        var t = EasyRemoteTester.create('Test connection basics',{});
        var data = {};



        t.add(showConfigs);


        t.add(function addFirstRecord() {
            nodeA.con.create({name: "roger"})
            nodeA.repo.compareTables('a_table', 'b_table', false);
            t.cb();
        });

        t.add(delayIfNotConverged_showConverged);

        t.wait(8)

        function saveState() {
            sh.each(dict2, function saveState(i, node){
                node.saveState();
            })
            t.cb();
        };

        t.add(saveState);
        t.add(writeLog);

        t.add(showCounts);
        t.add(showConverged);
        t.add(delayIfNotConverged_showConverged);
        t.add(function(){});

        function verifyTables() {
            var t2 = EasyRemoteTester.create('Test connection basics',{});
            var data = {};
            function compareTables(a, b) {
                var tableA = a+'_table';
                var tableB = b+'_table';
                if ( dict2[a]== null || dict2[b] == null) {
                    return function () {
                        t2.cb();
                    }
                    return;
                }
                return function () {
                    nodeA.repo.compareTables(tableA,tableB);
                    t2.cb();
                }
            }
            /*
             nodeA.repo.compareTables('a_table', 'b_table');
             nodeA.repo.compareTables('b_table', 'c_table');
             nodeA.repo.compareTables('a_table', 'b_table');
             nodeA.repo.compareTables('a_table', 'd_table');
             */
            t2.add(compareTables('a', 'b'))

            t2.add(compareTables('b', 'c'))
            t2.add(compareTables('a', 'b'))
            t2.add(compareTables('a', 'd'))
            t2.add(compareTables('a', 'e'))

            t2.fxDone = t.cb;
            t2.add(function endT(){t.cb()})

        }
        t.add(verifyTables);

        t.wait(2)
        t.add(function addFirstRecord() {
            nodeD.con.create({name: "roger-d."})
            // nodeA.repo.compareTables('a_table', 'b_table', false);
            t.cb();
        });
        t.wait(8+0);
        t.add(verifyTables);


        t.wait(2);
        t.add(function addFirstRecord() {
            dict2['e'].con.create({name: "roger-e."})
            t.cb();
        });
        t.wait(8+0);
        t.add(verifyTables);

        t.wait(1);

        function showTotals() {
            console.log('showTotals', 'dict2');
            sh.each(dict2, function saveState(i, node){
                console.log('output for', node.name);
                var moment = require('moment');
                node.data.convergedAgo = moment(node.data.convergedAt).fromNow();
                console.log(sh.toJSONString(node.data))
            })

            console.log('');
            /*sh.each(netw.globalLog, function showLog(i, logLine){
             console.log(i+1+':',logLine);
             })*/
            sh.each.print(netw.globalLog);

            netw.collectHTTPRequest('config', function onShowConfigs(logs){
                sh.each.print(logs)
                t.cb();
            })
        }
        t.add(showTotals);



        function showConfigs() {
            netw.collectHTTPRequest('config', function onShowConfigs(logs, logsDict){
                sh.each.print(logsDict)
                t.cb();
            })
        }
        t.add(showConfigs);


        function writeLog() {
            netw.writeLog('global_log.txt')
            t.cb();
        }
        function showCounts() {
            netw.collectHTTPRequest('counts', function onShowConfigs(logs, logsDict){
                sh.each.print(logsDict)
                t.cb();
            })
        }

        function showConverged() {
            netw.collectHTTPRequest('converged', function onShowConfigs(logs, logsDict){
                sh.each.print(logsDict)
                t.cb();
            });
        }

        var delayedCount = 0
        function delayIfNotConverged_showConverged(){
            netw.collectHTTPRequest('converged', function onShowConfigs(logs, logsDict){
                sh.each.print(logsDict)
                var converged = sh.each.find(  logs, false) == false ;
                var converged2 = sh.each.find( logs, 'false') == false ;
                if ( converged2 == false ) {
                    t.addNext(function () {
                        console.error('delayed ...');
                        t.cb();
                    })
                    t.addNext(t.wait(3,false),1);
                    t.addNext(delayIfNotConverged_showConverged,2);
                    delayedCount++
                    t.cb();
                    return;
                }
                console.error('delayedCount', delayedCount)
                t.cb();
            });
        }

        t.add(writeLog);

    }




    // }
    //sh.wait1Sec(createItems);
    //setTimeout(createItems, 3000)


}
