package org.apache.storm.solr.schema;

import java.io.Serializable;

/**
 * Created by hlouro on 7/27/15.
 */
public class CopyField implements Serializable {
    private String source;
    private String dest;

    public String getSource() {
        return source;
    }

    public String getDest() {
        return dest;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public void setDest(String dest) {
        this.dest = dest;
    }

    @Override
    public String toString() {
        return "CopyField{" +
                "source='" + source + '\'' +
                ", dest='" + dest + '\'' +
                '}';
    }
}
