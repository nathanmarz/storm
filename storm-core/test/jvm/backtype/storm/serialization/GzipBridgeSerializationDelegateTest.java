package backtype.storm.serialization;

import junit.framework.TestCase;

import java.io.Serializable;

public class GzipBridgeSerializationDelegateTest extends TestCase {

    SerializationDelegate testDelegate;

    public void setUp() throws Exception {
        super.setUp();
        testDelegate = new GzipBridgeSerializationDelegate();
    }

    public void testDeserialize_readingFromGzip() throws Exception {
        TestPojo pojo = new TestPojo();
        pojo.name = "foo";
        pojo.age = 100;

        byte[] serialized = new GzipSerializationDelegate().serialize(pojo);

        TestPojo pojo2 = (TestPojo)testDelegate.deserialize(serialized);

        assertEquals(pojo2.name, pojo.name);
        assertEquals(pojo2.age, pojo.age);
    }

    public void testDeserialize_readingFromGzipBridge() throws Exception {
        TestPojo pojo = new TestPojo();
        pojo.name = "bar";
        pojo.age = 200;

        byte[] serialized = new GzipBridgeSerializationDelegate().serialize(pojo);

        TestPojo pojo2 = (TestPojo)testDelegate.deserialize(serialized);

        assertEquals(pojo2.name, pojo.name);
        assertEquals(pojo2.age, pojo.age);
    }

    public void testDeserialize_readingFromDefault() throws Exception {
        TestPojo pojo = new TestPojo();
        pojo.name = "baz";
        pojo.age = 300;

        byte[] serialized = new DefaultSerializationDelegate().serialize(pojo);

        TestPojo pojo2 = (TestPojo)testDelegate.deserialize(serialized);

        assertEquals(pojo2.name, pojo.name);
        assertEquals(pojo2.age, pojo.age);
    }

    static class TestPojo implements Serializable {
        String name;
        int age;
    }
}