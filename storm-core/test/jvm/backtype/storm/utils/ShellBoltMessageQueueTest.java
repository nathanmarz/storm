package backtype.storm.utils;

import backtype.storm.multilang.BoltMsg;
import com.google.common.collect.Lists;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ShellBoltMessageQueueTest extends TestCase {
    @Test
    public void testPollTaskIdsFirst() throws InterruptedException {
        ShellBoltMessageQueue queue = new ShellBoltMessageQueue();

        // put bolt message first, then put task ids
        queue.putBoltMsg(new BoltMsg());
        ArrayList<Integer> taskIds = Lists.newArrayList(1, 2, 3);
        queue.putTaskIds(taskIds);

        Object msg = queue.poll(10, TimeUnit.SECONDS);

        // task ids should be pulled first
        assertTrue(msg instanceof List<?>);
        assertEquals(msg, taskIds);
    }

    @Test
    public void testPollWhileThereAreNoDataAvailable() throws InterruptedException {
        ShellBoltMessageQueue queue = new ShellBoltMessageQueue();

        long start = System.currentTimeMillis();
        Object msg = queue.poll(1, TimeUnit.SECONDS);
        long finish = System.currentTimeMillis();

        assertNull(msg);
        assertTrue(finish - start > 1000);
    }

    @Test
    public void testPollShouldReturnASAPWhenDataAvailable() throws InterruptedException {
        final ShellBoltMessageQueue queue = new ShellBoltMessageQueue();
        final List<Integer> taskIds = Lists.newArrayList(1, 2, 3);

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // NOOP
                }

                queue.putTaskIds(taskIds);
            }
        });
        t.start();

        long start = System.currentTimeMillis();
        Object msg = queue.poll(10, TimeUnit.SECONDS);
        long finish = System.currentTimeMillis();

        assertEquals(msg, taskIds);
        assertTrue(finish - start < (10 * 1000));
    }
}
