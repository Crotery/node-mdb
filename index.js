var spawn = require('child_process').spawn
var stream = require('stream')
var util = require('util')
var concat = require('concat-stream')
var byline = require('byline');

function Mdb(file) {
    stream.Stream.call(this)
    this.writable = true
    this.file = file
    this.tableDelimiter = ','
}

util.inherits(Mdb, stream.Stream)

Mdb.prototype.toCSV = function (table, cb) {
    var cmd = spawn('mdb-export', [this.file, table])
    cmd.stdout.pipe(
        concat(function (err, out) {
            if (err) return cb(err)
            if (!out) return cb('no output')
            cb(false, out.toString())
        })
    )
}
// Streaming reading of choosen columns in a table in a MDB file.
// parameters :


//   table : name of the table
//   columns : names of the desired columns
//   onRow : a callback accepting a row (an array of strings)
//   onDone : an optional callback called when everything is finished with an error code or 0 as argument
Mdb.prototype.queryTable = function (table, columns, onRow, onDone) {
    var cmd = spawn('mdb-export', [this.file, table, "-Q", '-d\\t\\t', '-R\\n']);
    cmd.stdout.setEncoding('utf8');
    var rowIndex = 0, colIndexes;
    byline(cmd.stdout).on('data', function (line) {
        var cells = line.toString().split('\t\t');
        if (!rowIndex++) { // first line, let's find the col indexes
            var lc = function (s) {
                return s.toLowerCase()
            };
            colIndexes = columns.map(lc).map(function (name) {
                return cells.map(lc).indexOf(name);
            });
        } else { // other lines, let's give to the callback the required cells
            var a = colIndexes.map(function (index) {
                return ~index ? cells[index] : null
            });
            var rs = {};
            for (var i = 0; i < columns.length; i++) {
                var name = columns[i];
                rs[name] = a[i];
            }

            onRow(rs);
        }
    });
    cmd.on('exit', function (code) {
        if (onDone) onDone(code);
    });
}
Mdb.prototype.toSQL = function (table, cb) {
    var cmd = spawn('mdb-export', ['-I -R ;\r\n', this.file, table])
    cmd.stdout.pipe(
        concat(function (err, out) {
            if (err) return cb(err)
            if (!out) return cb('no output')
            cb(false, out.toString())
        })
    )
}

Mdb.prototype.tables = function (cb) {
    var self = this
    var cmd = spawn('mdb-tables', ['-d' + this.tableDelimiter, this.file])
    cmd.stdout.pipe(
        concat(function (err, out) {
            if (err) return cb(err.toString())
            if (!out) return cb('no output')
            var tables = out.toString().replace(/,\n$/, '').split(self.tableDelimiter)
            cb(false, tables)
        })
    )
}

module.exports = function (data) {
    return new Mdb(data)
}
