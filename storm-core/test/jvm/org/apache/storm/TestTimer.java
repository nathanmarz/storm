package org.apache.storm;

import org.apache.storm.utils.Time;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jerrypeng on 2/9/16.
 */
public class TestTimer {
    //public static StormTimerTask mkTimer(TimerFunc onKill, String name) {
    private static final Logger LOG = LoggerFactory.getLogger(TestTimer.class);

    @Test
    public void testTimer() throws InterruptedException {
//        StormTimer.StormTimerTask task1 = StormTimer.mkTimer("timer", new StormTimer.TimerFunc() {
//            @Override
//            public void run(Object o) {
//                LOG.info("task1 onKill at {}", Time.currentTimeSecs());
//            }
//        });
//        StormTimer.scheduleRecurring(task1, 10, 5, new StormTimer.TimerFunc(){
//            @Override
//            public void run(Object o) {
//                LOG.info("task1-1 scheduleRecurring func at {}", Time.currentTimeSecs());
//            }
//        });
//        StormTimer.scheduleRecurring(task1, 5, 10, new StormTimer.TimerFunc(){
//            @Override
//            public void run(Object o) {
//                LOG.info("task1-2 scheduleRecurring func at {}", Time.currentTimeSecs());
//            }
//        });
//
//        StormTimer.StormTimerTask task2 = StormTimer.mkTimer("timer", new StormTimer.TimerFunc() {
//            @Override
//            public void run(Object o) {
//                LOG.info("task2 onKill at {}", Time.currentTimeSecs());
//            }
//        });
//        StormTimer.scheduleRecurringWithJitter(task2, 10, 5, 2000, new StormTimer.TimerFunc(){
//            @Override
//            public void run(Object o) {
//                LOG.info("task2 scheduleRecurringWithJitter func at {}", Time.currentTimeSecs());
//            }
//        });
//
//        LOG.info("sleeping...");
//        Time.sleep(30000);
//
//        LOG.info("canceling task");
//        StormTimer.cancelTimer(task1);
//        StormTimer.cancelTimer(task2);

    }
}
