package storm.kafka;

import java.util.Arrays;
import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;

public class MessageMetadataSchemeAsMultiScheme extends SchemeAsMultiScheme {
    private static final long serialVersionUID = -7172403703813625116L;

    public MessageMetadataSchemeAsMultiScheme(Scheme scheme) {
        super(scheme);
    }

    @SuppressWarnings("unchecked")
    public Iterable<List<Object>> deserializeMessageWithMetadata(byte[] message, Partition partition, long offset) {
        List<Object> o = ((MessageMetadataScheme) scheme).deserializeMessageWithMetadata(message, partition, offset);
        if (o == null) {
            return null;
        } else {
            return Arrays.asList(o);
        }
    }
}
