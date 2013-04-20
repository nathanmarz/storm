# -*- coding: utf-8 -*-
require File.expand_path("storm", File.dirname(__FILE__))

$words = ["nathan", "mike", "jackson", "golda", "bertels人"]

def random_word
  $words[rand($words.length)]
end

class TesterSpout < Storm::Spout
  attr_accessor :uid, :pending

  def open(conf, context)
    emit ['spout initializing']
    self.pending = {}
    self.uid = 0
  end

  def nextTuple
    sleep 0.5
    word = random_word
    id = self.uid += 1
    self.pending[id] = word
    emit [word], :id => id
  end

  def ack(id)
    self.pending.delete(id)
  end

  def fail(id)
    word = self.pending[id]
    log "emitting " + word + " on fail"
    emit [word], :id => id
  end
end

TesterSpout.new.run
