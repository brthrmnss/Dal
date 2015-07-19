/**
 * Created by user on 7/8/15.
 */
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


    connection.query("DROP DATABASE    test_sync;", function(err, rows) {
        // connected! (unless `err` is set)
      console.log('dropped database', err);
       // process.exit();
    });
});