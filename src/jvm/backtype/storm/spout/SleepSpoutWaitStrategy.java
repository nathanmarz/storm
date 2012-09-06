package backtype.storm.spout;

import backtype.storm.Config;
import java.util.Map;


public class SleepSpoutWaitStrategy implements ISpoutWaitStrategy {

    long sleepMillis;
    
    @Override
    public void prepare(Map conf) {
        sleepMillis = ((Number) conf.get(Config.TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS)).longValue();
    }

    @Override
    public void emptyEmit(long streak) {
        try {
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
