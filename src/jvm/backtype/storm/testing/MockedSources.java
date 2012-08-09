package backtype.storm.testing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class MockedSources {
	/**
	 * mocked spout sources for the [spout, stream] pair.
	 */
    private Map<String, List<FixedTuple>> data = new HashMap<String, List<FixedTuple>>();
    
    /**
     * add mock data for the spout.
     * 
     * @param spoutId the spout to be mocked
     * @param streamId the stream of the spout to be mocked
     * @param objects the mocked data
     */
    public void addMockData(String spoutId, String streamId, Values... valueses) {
        if (!data.containsKey(spoutId)) {
            data.put(spoutId, new ArrayList<FixedTuple>());
        }
        
        List<FixedTuple> tuples = data.get(spoutId);
        for (int i = 0; i < valueses.length; i++) {
            FixedTuple tuple = new FixedTuple(streamId, valueses[i]);
            tuples.add(tuple);
        }
    }
    
    public void addMockData(String spoutId, Values... valueses) {
        this.addMockData(spoutId, Utils.DEFAULT_STREAM_ID, valueses);
    }
    
    public Map<String, List<FixedTuple>> getData() {
        return this.data;
    }
}
