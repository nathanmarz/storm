package storm.dedup.impl;

import java.util.Arrays;

import storm.dedup.Bytes;

public class BytesArrayRef {
  private byte[] bytes;
  
  public BytesArrayRef(byte[] bytes) {
    this.bytes = bytes;
  }
  
  public byte[] getBytes() {
    return bytes;
  }
  
  public void setBytes(byte[] bytes) {
    this.bytes = bytes;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null)
      return false;
    
    if (!(obj instanceof BytesArrayRef))
      return false;
    
    // TODO Auto-generated method stub
    return Arrays.equals(getBytes(), ((BytesArrayRef)obj).getBytes());
  }

  @Override
  public int hashCode() {
    // TODO Auto-generated method stub
    return Arrays.hashCode(getBytes());
  }

  @Override
  public String toString() {
    // TODO Auto-generated method stub
    return Bytes.toString(getBytes());
  }
}
