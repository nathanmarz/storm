package org.apache.storm.solr.config;

import java.io.Serializable;

/**
 * Strategy definining when the Solr Bolt should commit the request to Solr.
 * <p></p>
 * Created by hlouro on 7/29/15.
 */
public interface SolrCommitStrategy extends Serializable {
    boolean commit();

    void update();
}
