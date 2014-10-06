var storm = require('./storm');
var BasicBolt = storm.BasicBolt;

function TesterBolt() {
    BasicBolt.call(this);
};

TesterBolt.prototype = Object.create(BasicBolt.prototype);
TesterBolt.prototype.constructor = TesterBolt;

TesterBolt.prototype.initialize = function(conf, context, done) {
//    this.emit({tuple: ['bolt initializing']}, function() {});
    done();
}

TesterBolt.prototype.process = function(tup, done) {
    var word = tup.values[0];

    if (Math.random() < 0.75) {
        this.emit({tuple: [word + 'lalala'], anchorTupleId: tup.id}, function() {});
        done();
    } else {
        this.log(word + ' randomly skipped!');
    }
}

new TesterBolt().run();