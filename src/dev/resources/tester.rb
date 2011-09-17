require File.expand_path("storm", File.dirname(__FILE__))

class TesterBolt < Storm::Bolt
  def process(tuple)
    emit [tuple.values[0] + "lalala"]
    ack tuple
  end
end

TesterBolt.new.run
