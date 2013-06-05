package backtype.storm.drpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public abstract class AbstractFixedSizePool<T> {

    public static Logger LOG = LoggerFactory.getLogger(AbstractFixedSizePool.class);

    protected final BlockingQueue<T> queue;

    protected AbstractFixedSizePool(int capacity) {
        this.queue = new LinkedBlockingQueue<T>(capacity);
    }

    protected T take() {
        try {
            return queue.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected T take(int timeout, TimeUnit timeUnit) {
        try {
            return queue.poll(timeout, timeUnit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected void put(T client) {
        try {
            queue.put(client);
        } catch (InterruptedException e) {
            LOG.warn("InterruptedException caught during finally clause; swallowing: ", e);
        }
    }
}
