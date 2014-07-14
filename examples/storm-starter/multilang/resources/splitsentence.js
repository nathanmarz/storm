/**
 * Bolt example - receives sentence and breaks it into words.
 */

var storm = require('./storm');
var BasicBolt = storm.BasicBolt;

function SplitSentenceBolt() {
    BasicBolt.call(this);
};

SplitSentenceBolt.prototype = Object.create(BasicBolt.prototype);
SplitSentenceBolt.prototype.constructor = SplitSentenceBolt;

SplitSentenceBolt.prototype.process = function(tup, done) {
        var self = this;
        var words = tup.values[0].split(" ");
        words.forEach(function(word) {
            self.emit({tuple: [word], anchorTupleId: tup.id}, function(taskIds) {
                self.log(word + ' sent to task ids - ' + taskIds);
            });
        });
        done();
}

new SplitSentenceBolt().run();