/**
 * Autogenerated by Thrift Compiler (0.7.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package backtype.storm.generated;

import org.apache.commons.lang.builder.HashCodeBuilder;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorInfo implements org.apache.thrift7.TBase<ErrorInfo, ErrorInfo._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift7.protocol.TStruct STRUCT_DESC = new org.apache.thrift7.protocol.TStruct("ErrorInfo");

  private static final org.apache.thrift7.protocol.TField ERROR_FIELD_DESC = new org.apache.thrift7.protocol.TField("error", org.apache.thrift7.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift7.protocol.TField ERROR_TIME_SECS_FIELD_DESC = new org.apache.thrift7.protocol.TField("error_time_secs", org.apache.thrift7.protocol.TType.I32, (short)2);

  private String error; // required
  private int error_time_secs; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift7.TFieldIdEnum {
    ERROR((short)1, "error"),
    ERROR_TIME_SECS((short)2, "error_time_secs");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // ERROR
          return ERROR;
        case 2: // ERROR_TIME_SECS
          return ERROR_TIME_SECS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __ERROR_TIME_SECS_ISSET_ID = 0;
  private BitSet __isset_bit_vector = new BitSet(1);

  public static final Map<_Fields, org.apache.thrift7.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift7.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift7.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.ERROR, new org.apache.thrift7.meta_data.FieldMetaData("error", org.apache.thrift7.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift7.meta_data.FieldValueMetaData(org.apache.thrift7.protocol.TType.STRING)));
    tmpMap.put(_Fields.ERROR_TIME_SECS, new org.apache.thrift7.meta_data.FieldMetaData("error_time_secs", org.apache.thrift7.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift7.meta_data.FieldValueMetaData(org.apache.thrift7.protocol.TType.I32)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift7.meta_data.FieldMetaData.addStructMetaDataMap(ErrorInfo.class, metaDataMap);
  }

  public ErrorInfo() {
  }

  public ErrorInfo(
    String error,
    int error_time_secs)
  {
    this();
    this.error = error;
    this.error_time_secs = error_time_secs;
    set_error_time_secs_isSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ErrorInfo(ErrorInfo other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.is_set_error()) {
      this.error = other.error;
    }
    this.error_time_secs = other.error_time_secs;
  }

  public ErrorInfo deepCopy() {
    return new ErrorInfo(this);
  }

  @Override
  public void clear() {
    this.error = null;
    set_error_time_secs_isSet(false);
    this.error_time_secs = 0;
  }

  public String get_error() {
    return this.error;
  }

  public void set_error(String error) {
    this.error = error;
  }

  public void unset_error() {
    this.error = null;
  }

  /** Returns true if field error is set (has been assigned a value) and false otherwise */
  public boolean is_set_error() {
    return this.error != null;
  }

  public void set_error_isSet(boolean value) {
    if (!value) {
      this.error = null;
    }
  }

  public int get_error_time_secs() {
    return this.error_time_secs;
  }

  public void set_error_time_secs(int error_time_secs) {
    this.error_time_secs = error_time_secs;
    set_error_time_secs_isSet(true);
  }

  public void unset_error_time_secs() {
    __isset_bit_vector.clear(__ERROR_TIME_SECS_ISSET_ID);
  }

  /** Returns true if field error_time_secs is set (has been assigned a value) and false otherwise */
  public boolean is_set_error_time_secs() {
    return __isset_bit_vector.get(__ERROR_TIME_SECS_ISSET_ID);
  }

  public void set_error_time_secs_isSet(boolean value) {
    __isset_bit_vector.set(__ERROR_TIME_SECS_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case ERROR:
      if (value == null) {
        unset_error();
      } else {
        set_error((String)value);
      }
      break;

    case ERROR_TIME_SECS:
      if (value == null) {
        unset_error_time_secs();
      } else {
        set_error_time_secs((Integer)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case ERROR:
      return get_error();

    case ERROR_TIME_SECS:
      return Integer.valueOf(get_error_time_secs());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case ERROR:
      return is_set_error();
    case ERROR_TIME_SECS:
      return is_set_error_time_secs();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ErrorInfo)
      return this.equals((ErrorInfo)that);
    return false;
  }

  public boolean equals(ErrorInfo that) {
    if (that == null)
      return false;

    boolean this_present_error = true && this.is_set_error();
    boolean that_present_error = true && that.is_set_error();
    if (this_present_error || that_present_error) {
      if (!(this_present_error && that_present_error))
        return false;
      if (!this.error.equals(that.error))
        return false;
    }

    boolean this_present_error_time_secs = true;
    boolean that_present_error_time_secs = true;
    if (this_present_error_time_secs || that_present_error_time_secs) {
      if (!(this_present_error_time_secs && that_present_error_time_secs))
        return false;
      if (this.error_time_secs != that.error_time_secs)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_error = true && (is_set_error());
    builder.append(present_error);
    if (present_error)
      builder.append(error);

    boolean present_error_time_secs = true;
    builder.append(present_error_time_secs);
    if (present_error_time_secs)
      builder.append(error_time_secs);

    return builder.toHashCode();
  }

  public int compareTo(ErrorInfo other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    ErrorInfo typedOther = (ErrorInfo)other;

    lastComparison = Boolean.valueOf(is_set_error()).compareTo(typedOther.is_set_error());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_error()) {
      lastComparison = org.apache.thrift7.TBaseHelper.compareTo(this.error, typedOther.error);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_error_time_secs()).compareTo(typedOther.is_set_error_time_secs());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_error_time_secs()) {
      lastComparison = org.apache.thrift7.TBaseHelper.compareTo(this.error_time_secs, typedOther.error_time_secs);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift7.protocol.TProtocol iprot) throws org.apache.thrift7.TException {
    org.apache.thrift7.protocol.TField field;
    iprot.readStructBegin();
    while (true)
    {
      field = iprot.readFieldBegin();
      if (field.type == org.apache.thrift7.protocol.TType.STOP) { 
        break;
      }
      switch (field.id) {
        case 1: // ERROR
          if (field.type == org.apache.thrift7.protocol.TType.STRING) {
            this.error = iprot.readString();
          } else { 
            org.apache.thrift7.protocol.TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 2: // ERROR_TIME_SECS
          if (field.type == org.apache.thrift7.protocol.TType.I32) {
            this.error_time_secs = iprot.readI32();
            set_error_time_secs_isSet(true);
          } else { 
            org.apache.thrift7.protocol.TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          org.apache.thrift7.protocol.TProtocolUtil.skip(iprot, field.type);
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();
    validate();
  }

  public void write(org.apache.thrift7.protocol.TProtocol oprot) throws org.apache.thrift7.TException {
    validate();

    oprot.writeStructBegin(STRUCT_DESC);
    if (this.error != null) {
      oprot.writeFieldBegin(ERROR_FIELD_DESC);
      oprot.writeString(this.error);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldBegin(ERROR_TIME_SECS_FIELD_DESC);
    oprot.writeI32(this.error_time_secs);
    oprot.writeFieldEnd();
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ErrorInfo(");
    boolean first = true;

    sb.append("error:");
    if (this.error == null) {
      sb.append("null");
    } else {
      sb.append(this.error);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("error_time_secs:");
    sb.append(this.error_time_secs);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift7.TException {
    // check for required fields
    if (!is_set_error()) {
      throw new org.apache.thrift7.protocol.TProtocolException("Required field 'error' is unset! Struct:" + toString());
    }

    if (!is_set_error_time_secs()) {
      throw new org.apache.thrift7.protocol.TProtocolException("Required field 'error_time_secs' is unset! Struct:" + toString());
    }

  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift7.protocol.TCompactProtocol(new org.apache.thrift7.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift7.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bit_vector = new BitSet(1);
      read(new org.apache.thrift7.protocol.TCompactProtocol(new org.apache.thrift7.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift7.TException te) {
      throw new java.io.IOException(te);
    }
  }

}

