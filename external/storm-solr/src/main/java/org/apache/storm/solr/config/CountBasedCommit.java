package org.apache.storm.solr.config;

/**
 * Class defining a count based commit strategy. When the count reaches the commit threshold,
 * SolrInputDocuments are committed to Solr.
 *
 * Created by hlouro on 7/29/15.
 */
public class CountBasedCommit implements SolrCommitStrategy {
    private int threshHold;
    private int count;

    /**
     * Initializes a count based commit strategy with the specified threshold
     *
     * @param threshold  The commit threshold, defining when SolrInputDocuments should be committed to Solr
     * */
    public CountBasedCommit(int threshold) {
        if (threshold < 1) {
            throw new IllegalArgumentException("Threshold must be a positive integer: " + threshold);
        }
        this.threshHold = threshold;
    }

    @Override
    public boolean commit() {
        return count != 0 && count % threshHold == 0;
    }


    @Override
    public void update() {
        count++;
    }

    public int getCount() {
        return count;
    }
}
