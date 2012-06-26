require File.expand_path("storm", File.dirname(__FILE__))

class Testerbolth < Storm::bolth
  def prepare(conf, context)
    emit ['bolth initializing']
  end

  def process(tuple)
    word = tuple.values[0]
    if (rand < 0.75)
      emit [word + "lalala"], :anchor => tuple
      ack tuple
    else
      log(word + ' randomly skipped!')
    end
  end
end

Testerbolth.new.run
