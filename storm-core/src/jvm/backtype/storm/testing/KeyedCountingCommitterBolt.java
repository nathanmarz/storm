package backtype.storm.testing;

import backtype.storm.transactional.ICommitter;

public class KeyedCountingCommitterBolt extends KeyedCountingBatchBolt implements ICommitter {

}
