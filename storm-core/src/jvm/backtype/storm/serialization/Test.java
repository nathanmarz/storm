package backtype.storm.serialization;

import backtype.storm.generated.SupervisorInfo;

/**
 * Created by pbrahmbhatt on 1/29/15.
 */
public class Test {

    public static void main(String[] args) throws Exception {
//        ThriftSerializationDelegate t = new ThriftSerializationDelegate();
//        t.serialize(null);

        DefaultSerializationDelegate d = new DefaultSerializationDelegate();
        System.out.println(d.serialize(null));
    }
}
