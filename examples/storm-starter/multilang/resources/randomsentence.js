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

/**
 * Example for storm spout. Emits random sentences.
 * The original class in java - org.apache.storm.starter.spout.RandomSentenceSpout.
 *
 */

var storm = require('./storm');
var Spout = storm.Spout;


var SENTENCES = [
    "the cow jumped over the moon",
    "an apple a day keeps the doctor away",
    "four score and seven years ago",
    "snow white and the seven dwarfs",
    "i am at two with nature"]

function RandomSentenceSpout(sentences) {
    Spout.call(this);
    this.runningTupleId = 0;
    this.sentences = sentences;
    this.pending = {};
};

RandomSentenceSpout.prototype = Object.create(Spout.prototype);
RandomSentenceSpout.prototype.constructor = RandomSentenceSpout;

RandomSentenceSpout.prototype.getRandomSentence = function() {
    return this.sentences[getRandomInt(0, this.sentences.length - 1)];
}

RandomSentenceSpout.prototype.nextTuple = function(done) {
    var self = this;
    var sentence = this.getRandomSentence();
    var tup = [sentence];
    var id = this.createNextTupleId();
    this.pending[id] = tup;
    //This timeout can be removed if TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS is configured to 100
    setTimeout(function() {
        self.emit({tuple: tup, id: id}, function(taskIds) {
            self.log(tup + ' sent to task ids - ' + taskIds);
        });
        done();
    },100);
}

RandomSentenceSpout.prototype.createNextTupleId = function() {
    var id = this.runningTupleId;
    this.runningTupleId++;
    return id;
}

RandomSentenceSpout.prototype.ack = function(id, done) {
    this.log('Received ack for - ' + id);
    delete this.pending[id];
    done();
}

RandomSentenceSpout.prototype.fail = function(id, done) {
    var self = this;
    this.log('Received fail for - ' + id + '. Retrying.');
    this.emit({tuple: this.pending[id], id:id}, function(taskIds) {
        self.log(self.pending[id] + ' sent to task ids - ' + taskIds);
    });
    done();
}

/**
 * Returns a random integer between min (inclusive) and max (inclusive)
 */
function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

new RandomSentenceSpout(SENTENCES).run();
