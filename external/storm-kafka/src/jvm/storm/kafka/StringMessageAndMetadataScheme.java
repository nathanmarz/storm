package storm.kafka;

import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class StringMessageAndMetadataScheme extends StringScheme implements MessageMetadataScheme {
    private static final long serialVersionUID = -5441841920447947374L;

    public static final String STRING_SCHEME_PARTITION_KEY = "partition";
    public static final String STRING_SCHEME_OFFSET = "offset";

    @Override
    public List<Object> deserializeMessageWithMetadata(byte[] message, Partition partition, long offset) {
        String stringMessage = StringScheme.deserializeString(message);
        return new Values(stringMessage, partition.partition, offset);
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(STRING_SCHEME_KEY, STRING_SCHEME_PARTITION_KEY, STRING_SCHEME_OFFSET);
    }

}
