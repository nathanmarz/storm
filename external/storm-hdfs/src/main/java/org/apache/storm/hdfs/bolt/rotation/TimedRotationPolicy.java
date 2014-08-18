package org.apache.storm.hdfs.bolt.rotation;

import backtype.storm.tuple.Tuple;

public class TimedRotationPolicy implements FileRotationPolicy {

    public static enum TimeUnit {

        SECONDS((long)1000),
        MINUTES((long)1000*60),
        HOURS((long)1000*60*60),
        DAYS((long)1000*60*60*24);

        private long milliSeconds;

        private TimeUnit(long milliSeconds){
            this.milliSeconds = milliSeconds;
        }

        public long getMilliSeconds(){
            return milliSeconds;
        }
    }

    private long interval;

    public TimedRotationPolicy(float count, TimeUnit units){
        this.interval = (long)(count * units.getMilliSeconds());
    }


    /**
     * Called for every tuple the HdfsBolt executes.
     *
     * @param tuple  The tuple executed.
     * @param offset current offset of file being written
     * @return true if a file rotation should be performed
     */
    @Override
    public boolean mark(Tuple tuple, long offset) {
        return false;
    }

    /**
     * Called after the HdfsBolt rotates a file.
     */
    @Override
    public void reset() {

    }

    public long getInterval(){
        return this.interval;
    }
}
