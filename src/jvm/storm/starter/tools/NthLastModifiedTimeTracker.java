package storm.starter.tools;

import org.apache.commons.collections.buffer.CircularFifoBuffer;

import backtype.storm.utils.Time;

/**
 * This class tracks the time-since-last-modify of a "thing" in a rolling fashion.
 * 
 * For example, create a 5-slot tracker to track the five most recent time-since-last-modify.
 * 
 * You must manually "mark" that the "something" that you want to track -- in terms of modification times -- has just
 * been modified.
 * 
 */
public class NthLastModifiedTimeTracker {

    private static final int MILLIS_IN_SEC = 1000;

    private final CircularFifoBuffer lastModifiedTimesMillis;

    public NthLastModifiedTimeTracker(int numTimesToTrack) {
        if (numTimesToTrack < 1) {
            throw new IllegalArgumentException("numTimesToTrack must be greater than zero (you requested "
                + numTimesToTrack + ")");
        }
        lastModifiedTimesMillis = new CircularFifoBuffer(numTimesToTrack);
        initLastModifiedTimesMillis();
    }

    private void initLastModifiedTimesMillis() {
        long nowCached = now();
        for (int i = 0; i < lastModifiedTimesMillis.maxSize(); i++) {
            lastModifiedTimesMillis.add(Long.valueOf(nowCached));
        }
    }

    private long now() {
        return Time.currentTimeMillis();
    }

    public int secondsSinceOldestModification() {
        long modifiedTimeMillis = ((Long) lastModifiedTimesMillis.get()).longValue();
        return (int) ((now() - modifiedTimeMillis) / MILLIS_IN_SEC);
    }

    public void markAsModified() {
        updateLastModifiedTime();
    }

    private void updateLastModifiedTime() {
        lastModifiedTimesMillis.add(now());
    }

}
