package backtype.storm.multilang;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import backtype.storm.task.TopologyContext;

public interface ISerializer extends Serializable {
	void initialize (OutputStream processIn, InputStream processOut);
	Number connect (Map conf, TopologyContext context) throws IOException, NoOutputException;
	Emission readEmission() throws IOException, NoOutputException;
	void writeImmission(Immission immission) throws IOException;
	void writeSpoutMsg(SpoutMsg msg) throws IOException;
	void writeTaskIds(List<Integer> taskIds) throws IOException;
}
