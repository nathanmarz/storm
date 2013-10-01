package backtype.storm.multilang;

import java.util.List;

public class Immission {
	private String id;
	private String comp;
	private String stream;
	private long task;
	private List<Object> tuple;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getComp() {
		return comp;
	}
	public void setComp(String comp) {
		this.comp = comp;
	}
	public String getStream() {
		return stream;
	}
	public void setStream(String stream) {
		this.stream = stream;
	}
	public long getTask() {
		return task;
	}
	public void setTask(long task) {
		this.task = task;
	}
	public List<Object> getTuple() {
		return tuple;
	}
	public void setTuple(List<Object> tuple) {
		this.tuple = tuple;
	}
}
