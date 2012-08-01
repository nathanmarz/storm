package storm.trident.operation.impl;

import org.apache.commons.lang.builder.ToStringBuilder;
import storm.trident.operation.TridentCollector;


//for ChainedAggregator
public class ChainedResult {
    Object[] objs;
    TridentCollector[] collectors;
    
    public ChainedResult(TridentCollector collector, int size) {
        objs = new Object[size];
        collectors = new TridentCollector[size];
        for(int i=0; i<size; i++) {
            if(size==1) {
                collectors[i] = collector;
            } else {
                collectors[i] = new CaptureCollector();                
            }
        }
    }
    
    public void setFollowThroughCollector(TridentCollector collector) {
        if(collectors.length>1) {
            for(TridentCollector c: collectors) {
                ((CaptureCollector) c).setCollector(collector);
            }
        }
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(objs);
    }    
}
