package backtype.storm.serialization;

import backtype.storm.Config;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;


public class SerializationFactory {
    public static final int SERIALIZATION_TOKEN_BOUNDARY = 32;
    public static Logger LOG = Logger.getLogger(SerializationFactory.class);
    private static byte[] EMPTY_BYTE_ARRAY = new byte[0];
    public static final int JAVA_SERIALIZATION_TOKEN = 1;

    private JavaSerialization _javaSerializer = new JavaSerialization();
    private boolean _useJavaSerialization;
    private Map<Integer, ISerialization> _serializations = new HashMap<Integer, ISerialization>() {{
        put(2, new ISerialization<Integer>() {
                public boolean accept(Class c) {
                    return Integer.class.equals(c);
                }

                public void serialize(Integer object, DataOutputStream stream) throws IOException {
                    stream.writeInt(object);
//                    WritableUtils.writeVInt(stream, object);
                }

                public Integer deserialize(DataInputStream stream) throws IOException {
//                    return WritableUtils.readVInt(stream);
                    return stream.readInt();
                }
        });
        put(3, new ISerialization<Long>() {
                public boolean accept(Class c) {
                    return Long.class.equals(c);
                }

                public void serialize(Long object, DataOutputStream stream) throws IOException {
                    stream.writeLong(object);
//                    WritableUtils.writeVLong(stream, object);
                }

                public Long deserialize(DataInputStream stream) throws IOException {
//                    return WritableUtils.readVLong(stream);
                    return stream.readLong();
                }
        });
        put(4, new ISerialization<Float>() {
                public boolean accept(Class c) {
                    return Float.class.equals(c);
                }

                public void serialize(Float object, DataOutputStream stream) throws IOException {
                    stream.writeFloat(object);
                }

                public Float deserialize(DataInputStream stream) throws IOException {
                    return stream.readFloat();
                }
        });
        put(5, new ISerialization<Double>() {
                public boolean accept(Class c) {
                    return Double.class.equals(c);
                }

                public void serialize(Double object, DataOutputStream stream) throws IOException {
                    stream.writeDouble(object);
                }

                public Double deserialize(DataInputStream stream) throws IOException {
                    return stream.readDouble();
                }
        });
        put(6, new ISerialization<Byte>() {
                public boolean accept(Class c) {
                    return Byte.class.equals(c);
                }

                public void serialize(Byte object, DataOutputStream stream) throws IOException {
                    stream.writeByte(object);
                }

                public Byte deserialize(DataInputStream stream) throws IOException {
                    return stream.readByte();
                }
        });
        put(7, new ISerialization<Short>() {
                public boolean accept(Class c) {
                    return Short.class.equals(c);
                }

                public void serialize(Short object, DataOutputStream stream) throws IOException {
                    stream.writeShort(object);
                }

                public Short deserialize(DataInputStream stream) throws IOException {
                    return stream.readShort();
                }
        });
        put(8, new ISerialization<String>() {
                final static int SERIALIZATION_LIMIT_SIZE = 64 * 1024 - 1;
            
                public boolean accept(Class c) {
                    return String.class.equals(c);
                }

                public void serialize(String object, DataOutputStream stream) throws IOException {
                    // this hack is here because writeUTF doesn't work for strings greater than 64K
                    // but writeUTF seems to be twice than writing the byte array directly, so there's
                    // a switch here based on the size of the string
                    if(object.length()<=SERIALIZATION_LIMIT_SIZE) {
                        stream.writeInt(-1);
                        stream.writeUTF(object);
                    } else {
                        byte[] bytes = object.getBytes();
                        stream.writeInt(bytes.length);
                        stream.write(bytes);
                    }
                }

                public String deserialize(DataInputStream stream) throws IOException {
                    int size = stream.readInt();
                    if(size==-1) {
                        return stream.readUTF();                    
                    } else {
                        byte[] bytes = new byte[size];
                        stream.readFully(bytes);
                        return new String(bytes);
                    }
                }
        });
        put(9, new ISerialization<Boolean>() {
                public boolean accept(Class c) {
                    return Boolean.class.equals(c);
                }

                public void serialize(Boolean object, DataOutputStream stream) throws IOException {
                    stream.writeBoolean(object);
                }

                public Boolean deserialize(DataInputStream stream) throws IOException {
                    return stream.readBoolean();
                }
        });
        put(10, new ISerialization<byte[]>() {            
                public boolean accept(Class c) {
                    return EMPTY_BYTE_ARRAY.getClass().equals(c);
                }

                public void serialize(byte[] object, DataOutputStream stream) throws IOException {
//                    WritableUtils.writeVInt(stream, object.length);
                    stream.writeInt(object.length);
                    stream.write(object, 0, object.length);
                }

                public byte[] deserialize(DataInputStream stream) throws IOException {
//                    int size = WritableUtils.readVInt(stream);
                    int size = stream.readInt();
                    byte[] ret = new byte[size];
                    stream.readFully(ret);
                    return ret;
                }
        });
    }};

    public SerializationFactory(Map conf) {
        _useJavaSerialization = (Boolean) conf.get(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION);
        boolean skipMissing = (Boolean) conf.get(Config.TOPOLOGY_SKIP_MISSING_SERIALIZATIONS);
        Map<Object, String> customSerializations = (Map<Object, String>) conf.get(Config.TOPOLOGY_SERIALIZATIONS);
        if(customSerializations==null) customSerializations = new HashMap<Object, String>();
        for(Object tokenObj: customSerializations.keySet()) {
            String serializationClassName = customSerializations.get(tokenObj);
            int token = toToken(tokenObj);
            if(token<=SERIALIZATION_TOKEN_BOUNDARY) {
                throw new RuntimeException("Illegal token " + token + " for " + serializationClassName);
            }
            try {
                LOG.info("Loading custom serialization " + serializationClassName + " for token " + token);
                Class serClass = Class.forName(serializationClassName);
                _serializations.put(token, (ISerialization) serClass.newInstance());
            } catch(ClassNotFoundException e) {
                if(skipMissing) {
                    LOG.info("Could not find serialization for " + serializationClassName + ". Skipping...");
                } else {
                    throw new RuntimeException(e);
                }
            } catch(InstantiationException e) {
                throw new RuntimeException(e);
            } catch(IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        if(_serializations.containsKey(JAVA_SERIALIZATION_TOKEN)) {
            throw new RuntimeException("Should not have a serialization with the java serialization token");
        }
    }

    private int toToken(Object tokenObj) {
        if(tokenObj instanceof Long) {
            return ((Long) tokenObj).intValue();
        } else if(tokenObj instanceof Integer) {
            return (Integer) tokenObj;
        } else if (tokenObj instanceof String) {
            // this special case exists because user's storm conf gets serialized to json, which will convert numbered keys into strings
            // TODO: replace use of json in nimbus storm submit with yaml
            return Integer.parseInt((String) tokenObj);
        } else {
            throw new RuntimeException("Unexpected token class " + tokenObj + " " + tokenObj.getClass().toString());
        }
    }


    public FieldSerialization getSerializationForToken(int token) {
        ISerialization ser = _serializations.get(token);
        if(ser==null) {
            if(token==JAVA_SERIALIZATION_TOKEN) {
                ser = _javaSerializer;
            } else {
                throw new RuntimeException("Could not find serialization for token " + token);
            }
        }
        return new FieldSerialization(token, ser);
    }

    public FieldSerialization getSerializationForClass(Class klass) {
        for(int token: _serializations.keySet()) {
            ISerialization ser = _serializations.get(token);
            if(ser.accept(klass)) {
                return getSerializationForToken(token);
            }
        }
        if(_useJavaSerialization && _javaSerializer.accept(klass)) {
            return new FieldSerialization(JAVA_SERIALIZATION_TOKEN, _javaSerializer);
        }
        throw new RuntimeException("Could not find serialization for class " + klass.toString());
    }
}
