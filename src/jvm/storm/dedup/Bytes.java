package storm.dedup;

import java.io.UnsupportedEncodingException;

public class Bytes {
  
  public static final String UTF8 = "UTF8";
  
  public static  byte[] toBytes(String string) {
    try {
      return string.getBytes(UTF8);
    } catch (UnsupportedEncodingException e) {
      return null;
    }
  }
  
  public static String toString(byte[] bytes) {
    try {
      return new String(bytes, UTF8);
    } catch (UnsupportedEncodingException e) {
      return null;
    }
  }
}
