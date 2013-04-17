package backtype.storm.testing;

import java.util.ArrayList;
import java.util.List;

public class MkTupleParam {
	private String stream;
	private String component;
	private List<String> fields;
	
	public String getStream() {
		return stream;
	}
	public void setStream(String stream) {
		this.stream = stream;
	}
	
	public String getComponent() {
		return component;
	}
	public void setComponent(String component) {
		this.component = component;
	}
	
	public List<String> getFields() {
		return fields;
	}
	public void setFields(String... fields) {
		this.fields = new ArrayList<String>();
		for (int i = 0; i < fields.length; i++) {
			this.fields.add(fields[i]);
		}
	}
}
