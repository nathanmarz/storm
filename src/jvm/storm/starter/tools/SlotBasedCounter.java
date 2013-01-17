package storm.starter.tools;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class provides per-slot counts of the occurrences of objects.
 * 
 * It can be used, for instance, as a building block for implementing sliding window counting of objects.
 * 
 * @param <T>
 *            The type of those objects we want to count.
 */
public final class SlotBasedCounter<T> implements Serializable {

    private static final long serialVersionUID = 4858185737378394432L;

    private final Map<T, long[]> objToCounts = new HashMap<T, long[]>();
    private final int numSlots;

    public SlotBasedCounter(int numSlots) {
        if (numSlots <= 0) {
            throw new IllegalArgumentException("Number of slots must be greater than zero (you requested " + numSlots
                + ")");
        }
        this.numSlots = numSlots;
    }

    public void incrementCount(T obj, int slot) {
        long[] counts = objToCounts.get(obj);
        if (counts == null) {
            counts = new long[this.numSlots];
            objToCounts.put(obj, counts);
        }
        counts[slot]++;
    }

    public long getCount(T obj, int slot) {
        long[] counts = objToCounts.get(obj);
        if (counts == null) {
            return 0;
        }
        else {
            return counts[slot];
        }
    }

    public Map<T, Long> getCounts() {
        Map<T, Long> result = new HashMap<T, Long>();
        for (T obj : objToCounts.keySet()) {
            result.put(obj, computeTotalCount(obj));
        }
        return result;
    }

    private long computeTotalCount(T obj) {
        long[] curr = objToCounts.get(obj);
        long total = 0;
        for (long l : curr) {
            total += l;
        }
        return total;
    }

    /**
     * Reset the slot count of any tracked objects to zero for the given slot.
     * 
     * @param slot
     */
    public void wipeSlot(int slot) {
        for (T obj : objToCounts.keySet()) {
            resetSlotCountToZero(obj, slot);
        }
    }

    private void resetSlotCountToZero(T obj, int slot) {
        long[] counts = objToCounts.get(obj);
        counts[slot] = 0;
    }

    private boolean shouldBeRemovedFromCounter(T obj) {
        return computeTotalCount(obj) == 0;
    }

    /**
     * Remove any object from the counter whose total count is zero (to free up memory).
     */
    public void wipeZeros() {
        Set<T> objToBeRemoved = new HashSet<T>();
        for (T obj : objToCounts.keySet()) {
            if (shouldBeRemovedFromCounter(obj)) {
                objToBeRemoved.add(obj);
            }
        }
        for (T obj : objToBeRemoved) {
            objToCounts.remove(obj);
        }
    }

}
