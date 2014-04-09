package storm.kafka;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class StringScheme implements Scheme {

    public static final String STRING_SCHEME_KEY = "str";

    public List<Object> deserialize(byte[] bytes) {
        return new Values(deserializeString(bytes));
    }

    public static String deserializeString(byte[] string) {
        try {
            return new String(string, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public Fields getOutputFields() {
        return new Fields(STRING_SCHEME_KEY);
    }
}