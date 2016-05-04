/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
var storm = require('./storm');
var Spout = storm.Spout;

var words = ["nathan", "mike", "jackson", "golda", "bertels人", "עברית"]

function TesterSpout() {
    Spout.call(this);
    this.runningTupleId = 0;
    this.pending = {};
};

TesterSpout.prototype = Object.create(Spout.prototype);
TesterSpout.prototype.constructor = TesterSpout;

TesterSpout.prototype.initialize = function(conf, context, done) {
//    this.emit({tuple: ['spout initializing']}, function() {});
    done();
}

TesterSpout.prototype.nextTuple = function(done) {
    var self = this;
    var word = this.getRandomWord();
    var tup = [word];
    var id = this.createNextTupleId();
    this.pending[id] = tup;
    this.emit({tuple: tup, id: id}, function(taskIds) {});
    done();
}

TesterSpout.prototype.createNextTupleId = function() {
    var id = this.runningTupleId;
    this.runningTupleId++;
    return id;
}

TesterSpout.prototype.ack = function(id, done) {
    delete this.pending[id];
    done();
}

TesterSpout.prototype.fail = function(id, done) {
    this.log("emitting " + this.pending[id] + " on fail");
    this.emit({tuple: this.pending[id], id: id}, function() {});
    done();
}

TesterSpout.prototype.getRandomWord = function() {
    return words[getRandomInt(0, words.length - 1)];
}

/**
 * Returns a random integer between min (inclusive) and max (inclusive)
 */
function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

new TesterSpout().run();
