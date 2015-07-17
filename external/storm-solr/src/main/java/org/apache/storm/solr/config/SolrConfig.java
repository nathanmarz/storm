package org.apache.storm.solr.config;

import org.apache.solr.client.solrj.SolrClient;

import java.io.Serializable;

/**
 * Class containing Solr configuration to be made available to Storm Solr bolts. Any configuration needed in
 * the bolts should be put in this class.
 * <p></p>
 * Created by hlouro on 7/29/15.
 */
public class SolrConfig implements Serializable {
    private String zkHostString;

    /**
     * @param zkHostString Zookeeper host string as defined in the {@link SolrClient} constructor
     * */
    public SolrConfig(String zkHostString) {
        this.zkHostString = zkHostString;
    }

    public String getZkHostString() {
        return zkHostString;
    }
}
