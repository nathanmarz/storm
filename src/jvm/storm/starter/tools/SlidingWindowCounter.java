package storm.starter.tools;

import java.io.Serializable;
import java.util.Map;

/**
 * This class counts objects in a sliding window fashion.
 * 
 * It is designed 1) to give multiple "producer" threads write access to the counter, i.e. being able to increment
 * counts of objects, and 2) to give a single "consumer" thread (e.g. {@link PeriodicSlidingWindowCounter}) read access
 * to the counter. Whenever the consumer thread performs a read operation, this class will advance the head slot of the
 * sliding window counter. This means that the consumer thread indirectly controls where writes of the producer threads
 * will go to. Also, by itself this class will not advance the head slot.
 * 
 * A note for analyzing data based on a sliding window count: During the initial <code>windowLengthInSlots</code>
 * iterations, this sliding window counter will always return object counts that are equal or greater than in the
 * previous iteration. This is the effect of the counter "loading up" at the very start of its existence. Conceptually,
 * this is the desired behavior.
 * 
 * To give an example, using a counter with 5 slots which for the sake of this example represent 1 minute of time each:
 * 
 * <pre>
 * {@code
 * Sliding window counts of an object X over time
 * 
 * Minute (timeline):
 * 1    2   3   4   5   6   7   8
 * 
 * Observed counts per minute:
 * 1    1   1   1   0   0   0   0
 * 
 * Counts returned by counter:
 * 1    2   3   4   4   3   2   1
 * }
 * </pre>
 * 
 * As you can see in this example, for the first <code>windowLengthInSlots</code> (here: the first five minutes) the
 * counter will always return counts equal or greater than in the previous iteration (1, 2, 3, 4, 4). This initial load
 * effect needs to be accounted for whenever you want to perform analyses such as trending topics; otherwise your
 * analysis algorithm might falsely identify the object to be trending as the counter seems to observe continuously
 * increasing counts. Also, note that during the initial load phase <em>every object</em> will exhibit increasing
 * counts.
 * 
 * On a high-level, the counter exhibits the following behavior: If you asked the example counter after two minutes,
 * "how often did you count the object during the past five minutes?", then it should reply
 * "I have counted it 2 times in the past five minutes", implying that it can only account for the last two of those
 * five minutes because the counter was not running before that time.
 * 
 * @param <T>
 *            The type of those objects we want to count.
 */
public final class SlidingWindowCounter<T> implements Serializable {

    private static final long serialVersionUID = -2645063988768785810L;

    private SlotBasedCounter<T> objCounter;
    private int headSlot;
    private int tailSlot;
    private int windowLengthInSlots;

    public SlidingWindowCounter(int windowLengthInSlots) {
        if (windowLengthInSlots < 2) {
            throw new IllegalArgumentException("Window length in slots must be at least two (you requested "
                + windowLengthInSlots + ")");
        }
        this.windowLengthInSlots = windowLengthInSlots;
        this.objCounter = new SlotBasedCounter<T>(this.windowLengthInSlots);

        this.headSlot = 0;
        this.tailSlot = slotAfter(headSlot);
    }

    public void incrementCount(T obj) {
        objCounter.incrementCount(obj, headSlot);
    }

    /**
     * Return the current (total) counts of all tracked objects, then advance the window.
     * 
     * Whenever this method is called, we consider the counts of the current sliding window to be available to and
     * successfully processed "upstream" (i.e. by the caller). Knowing this we will start counting any subsequent
     * objects within the next "chunk" of the sliding window.
     * 
     * @return
     */
    public Map<T, Long> getCountsThenAdvanceWindow() {
        Map<T, Long> counts = objCounter.getCounts();
        objCounter.wipeZeros();
        objCounter.wipeSlot(tailSlot);
        advanceHead();
        return counts;
    }

    private void advanceHead() {
        headSlot = tailSlot;
        tailSlot = slotAfter(tailSlot);
    }

    private int slotAfter(int slot) {
        return (slot + 1) % windowLengthInSlots;
    }

}
