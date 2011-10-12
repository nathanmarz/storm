/**
 * Autogenerated by Thrift
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

public class SpoutStats implements org.apache.thrift.TBase<SpoutStats, SpoutStats._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("SpoutStats");

  private static final org.apache.thrift.protocol.TField ACKED_FIELD_DESC = new org.apache.thrift.protocol.TField("acked", org.apache.thrift.protocol.TType.MAP, (short)1);
  private static final org.apache.thrift.protocol.TField FAILED_FIELD_DESC = new org.apache.thrift.protocol.TField("failed", org.apache.thrift.protocol.TType.MAP, (short)2);
  private static final org.apache.thrift.protocol.TField COMPLETE_MS_AVG_FIELD_DESC = new org.apache.thrift.protocol.TField("complete_ms_avg", org.apache.thrift.protocol.TType.MAP, (short)3);

  private Map<String,Map<Integer,Long>> acked;
  private Map<String,Map<Integer,Long>> failed;
  private Map<String,Map<Integer,Double>> complete_ms_avg;

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    ACKED((short)1, "acked"),
    FAILED((short)2, "failed"),
    COMPLETE_MS_AVG((short)3, "complete_ms_avg");

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
        case 1: // ACKED
          return ACKED;
        case 2: // FAILED
          return FAILED;
        case 3: // COMPLETE_MS_AVG
          return COMPLETE_MS_AVG;
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

  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.ACKED, new org.apache.thrift.meta_data.FieldMetaData("acked", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32), 
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)))));
    tmpMap.put(_Fields.FAILED, new org.apache.thrift.meta_data.FieldMetaData("failed", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32), 
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)))));
    tmpMap.put(_Fields.COMPLETE_MS_AVG, new org.apache.thrift.meta_data.FieldMetaData("complete_ms_avg", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32), 
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.DOUBLE)))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(SpoutStats.class, metaDataMap);
  }

  public SpoutStats() {
  }

  public SpoutStats(
    Map<String,Map<Integer,Long>> acked,
    Map<String,Map<Integer,Long>> failed,
    Map<String,Map<Integer,Double>> complete_ms_avg)
  {
    this();
    this.acked = acked;
    this.failed = failed;
    this.complete_ms_avg = complete_ms_avg;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public SpoutStats(SpoutStats other) {
    if (other.is_set_acked()) {
      Map<String,Map<Integer,Long>> __this__acked = new HashMap<String,Map<Integer,Long>>();
      for (Map.Entry<String, Map<Integer,Long>> other_element : other.acked.entrySet()) {

        String other_element_key = other_element.getKey();
        Map<Integer,Long> other_element_value = other_element.getValue();

        String __this__acked_copy_key = other_element_key;

        Map<Integer,Long> __this__acked_copy_value = new HashMap<Integer,Long>();
        for (Map.Entry<Integer, Long> other_element_value_element : other_element_value.entrySet()) {

          Integer other_element_value_element_key = other_element_value_element.getKey();
          Long other_element_value_element_value = other_element_value_element.getValue();

          Integer __this__acked_copy_value_copy_key = other_element_value_element_key;

          Long __this__acked_copy_value_copy_value = other_element_value_element_value;

          __this__acked_copy_value.put(__this__acked_copy_value_copy_key, __this__acked_copy_value_copy_value);
        }

        __this__acked.put(__this__acked_copy_key, __this__acked_copy_value);
      }
      this.acked = __this__acked;
    }
    if (other.is_set_failed()) {
      Map<String,Map<Integer,Long>> __this__failed = new HashMap<String,Map<Integer,Long>>();
      for (Map.Entry<String, Map<Integer,Long>> other_element : other.failed.entrySet()) {

        String other_element_key = other_element.getKey();
        Map<Integer,Long> other_element_value = other_element.getValue();

        String __this__failed_copy_key = other_element_key;

        Map<Integer,Long> __this__failed_copy_value = new HashMap<Integer,Long>();
        for (Map.Entry<Integer, Long> other_element_value_element : other_element_value.entrySet()) {

          Integer other_element_value_element_key = other_element_value_element.getKey();
          Long other_element_value_element_value = other_element_value_element.getValue();

          Integer __this__failed_copy_value_copy_key = other_element_value_element_key;

          Long __this__failed_copy_value_copy_value = other_element_value_element_value;

          __this__failed_copy_value.put(__this__failed_copy_value_copy_key, __this__failed_copy_value_copy_value);
        }

        __this__failed.put(__this__failed_copy_key, __this__failed_copy_value);
      }
      this.failed = __this__failed;
    }
    if (other.is_set_complete_ms_avg()) {
      Map<String,Map<Integer,Double>> __this__complete_ms_avg = new HashMap<String,Map<Integer,Double>>();
      for (Map.Entry<String, Map<Integer,Double>> other_element : other.complete_ms_avg.entrySet()) {

        String other_element_key = other_element.getKey();
        Map<Integer,Double> other_element_value = other_element.getValue();

        String __this__complete_ms_avg_copy_key = other_element_key;

        Map<Integer,Double> __this__complete_ms_avg_copy_value = new HashMap<Integer,Double>();
        for (Map.Entry<Integer, Double> other_element_value_element : other_element_value.entrySet()) {

          Integer other_element_value_element_key = other_element_value_element.getKey();
          Double other_element_value_element_value = other_element_value_element.getValue();

          Integer __this__complete_ms_avg_copy_value_copy_key = other_element_value_element_key;

          Double __this__complete_ms_avg_copy_value_copy_value = other_element_value_element_value;

          __this__complete_ms_avg_copy_value.put(__this__complete_ms_avg_copy_value_copy_key, __this__complete_ms_avg_copy_value_copy_value);
        }

        __this__complete_ms_avg.put(__this__complete_ms_avg_copy_key, __this__complete_ms_avg_copy_value);
      }
      this.complete_ms_avg = __this__complete_ms_avg;
    }
  }

  public SpoutStats deepCopy() {
    return new SpoutStats(this);
  }

  @Override
  public void clear() {
    this.acked = null;
    this.failed = null;
    this.complete_ms_avg = null;
  }

  public int get_acked_size() {
    return (this.acked == null) ? 0 : this.acked.size();
  }

  public void put_to_acked(String key, Map<Integer,Long> val) {
    if (this.acked == null) {
      this.acked = new HashMap<String,Map<Integer,Long>>();
    }
    this.acked.put(key, val);
  }

  public Map<String,Map<Integer,Long>> get_acked() {
    return this.acked;
  }

  public void set_acked(Map<String,Map<Integer,Long>> acked) {
    this.acked = acked;
  }

  public void unset_acked() {
    this.acked = null;
  }

  /** Returns true if field acked is set (has been assigned a value) and false otherwise */
  public boolean is_set_acked() {
    return this.acked != null;
  }

  public void set_acked_isSet(boolean value) {
    if (!value) {
      this.acked = null;
    }
  }

  public int get_failed_size() {
    return (this.failed == null) ? 0 : this.failed.size();
  }

  public void put_to_failed(String key, Map<Integer,Long> val) {
    if (this.failed == null) {
      this.failed = new HashMap<String,Map<Integer,Long>>();
    }
    this.failed.put(key, val);
  }

  public Map<String,Map<Integer,Long>> get_failed() {
    return this.failed;
  }

  public void set_failed(Map<String,Map<Integer,Long>> failed) {
    this.failed = failed;
  }

  public void unset_failed() {
    this.failed = null;
  }

  /** Returns true if field failed is set (has been assigned a value) and false otherwise */
  public boolean is_set_failed() {
    return this.failed != null;
  }

  public void set_failed_isSet(boolean value) {
    if (!value) {
      this.failed = null;
    }
  }

  public int get_complete_ms_avg_size() {
    return (this.complete_ms_avg == null) ? 0 : this.complete_ms_avg.size();
  }

  public void put_to_complete_ms_avg(String key, Map<Integer,Double> val) {
    if (this.complete_ms_avg == null) {
      this.complete_ms_avg = new HashMap<String,Map<Integer,Double>>();
    }
    this.complete_ms_avg.put(key, val);
  }

  public Map<String,Map<Integer,Double>> get_complete_ms_avg() {
    return this.complete_ms_avg;
  }

  public void set_complete_ms_avg(Map<String,Map<Integer,Double>> complete_ms_avg) {
    this.complete_ms_avg = complete_ms_avg;
  }

  public void unset_complete_ms_avg() {
    this.complete_ms_avg = null;
  }

  /** Returns true if field complete_ms_avg is set (has been assigned a value) and false otherwise */
  public boolean is_set_complete_ms_avg() {
    return this.complete_ms_avg != null;
  }

  public void set_complete_ms_avg_isSet(boolean value) {
    if (!value) {
      this.complete_ms_avg = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case ACKED:
      if (value == null) {
        unset_acked();
      } else {
        set_acked((Map<String,Map<Integer,Long>>)value);
      }
      break;

    case FAILED:
      if (value == null) {
        unset_failed();
      } else {
        set_failed((Map<String,Map<Integer,Long>>)value);
      }
      break;

    case COMPLETE_MS_AVG:
      if (value == null) {
        unset_complete_ms_avg();
      } else {
        set_complete_ms_avg((Map<String,Map<Integer,Double>>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case ACKED:
      return get_acked();

    case FAILED:
      return get_failed();

    case COMPLETE_MS_AVG:
      return get_complete_ms_avg();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case ACKED:
      return is_set_acked();
    case FAILED:
      return is_set_failed();
    case COMPLETE_MS_AVG:
      return is_set_complete_ms_avg();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof SpoutStats)
      return this.equals((SpoutStats)that);
    return false;
  }

  public boolean equals(SpoutStats that) {
    if (that == null)
      return false;

    boolean this_present_acked = true && this.is_set_acked();
    boolean that_present_acked = true && that.is_set_acked();
    if (this_present_acked || that_present_acked) {
      if (!(this_present_acked && that_present_acked))
        return false;
      if (!this.acked.equals(that.acked))
        return false;
    }

    boolean this_present_failed = true && this.is_set_failed();
    boolean that_present_failed = true && that.is_set_failed();
    if (this_present_failed || that_present_failed) {
      if (!(this_present_failed && that_present_failed))
        return false;
      if (!this.failed.equals(that.failed))
        return false;
    }

    boolean this_present_complete_ms_avg = true && this.is_set_complete_ms_avg();
    boolean that_present_complete_ms_avg = true && that.is_set_complete_ms_avg();
    if (this_present_complete_ms_avg || that_present_complete_ms_avg) {
      if (!(this_present_complete_ms_avg && that_present_complete_ms_avg))
        return false;
      if (!this.complete_ms_avg.equals(that.complete_ms_avg))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_acked = true && (is_set_acked());
    builder.append(present_acked);
    if (present_acked)
      builder.append(acked);

    boolean present_failed = true && (is_set_failed());
    builder.append(present_failed);
    if (present_failed)
      builder.append(failed);

    boolean present_complete_ms_avg = true && (is_set_complete_ms_avg());
    builder.append(present_complete_ms_avg);
    if (present_complete_ms_avg)
      builder.append(complete_ms_avg);

    return builder.toHashCode();
  }

  public int compareTo(SpoutStats other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    SpoutStats typedOther = (SpoutStats)other;

    lastComparison = Boolean.valueOf(is_set_acked()).compareTo(typedOther.is_set_acked());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_acked()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.acked, typedOther.acked);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_failed()).compareTo(typedOther.is_set_failed());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_failed()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.failed, typedOther.failed);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_complete_ms_avg()).compareTo(typedOther.is_set_complete_ms_avg());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_complete_ms_avg()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.complete_ms_avg, typedOther.complete_ms_avg);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    org.apache.thrift.protocol.TField field;
    iprot.readStructBegin();
    while (true)
    {
      field = iprot.readFieldBegin();
      if (field.type == org.apache.thrift.protocol.TType.STOP) { 
        break;
      }
      switch (field.id) {
        case 1: // ACKED
          if (field.type == org.apache.thrift.protocol.TType.MAP) {
            {
              org.apache.thrift.protocol.TMap _map71 = iprot.readMapBegin();
              this.acked = new HashMap<String,Map<Integer,Long>>(2*_map71.size);
              for (int _i72 = 0; _i72 < _map71.size; ++_i72)
              {
                String _key73;
                Map<Integer,Long> _val74;
                _key73 = iprot.readString();
                {
                  org.apache.thrift.protocol.TMap _map75 = iprot.readMapBegin();
                  _val74 = new HashMap<Integer,Long>(2*_map75.size);
                  for (int _i76 = 0; _i76 < _map75.size; ++_i76)
                  {
                    int _key77;
                    long _val78;
                    _key77 = iprot.readI32();
                    _val78 = iprot.readI64();
                    _val74.put(_key77, _val78);
                  }
                  iprot.readMapEnd();
                }
                this.acked.put(_key73, _val74);
              }
              iprot.readMapEnd();
            }
          } else { 
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 2: // FAILED
          if (field.type == org.apache.thrift.protocol.TType.MAP) {
            {
              org.apache.thrift.protocol.TMap _map79 = iprot.readMapBegin();
              this.failed = new HashMap<String,Map<Integer,Long>>(2*_map79.size);
              for (int _i80 = 0; _i80 < _map79.size; ++_i80)
              {
                String _key81;
                Map<Integer,Long> _val82;
                _key81 = iprot.readString();
                {
                  org.apache.thrift.protocol.TMap _map83 = iprot.readMapBegin();
                  _val82 = new HashMap<Integer,Long>(2*_map83.size);
                  for (int _i84 = 0; _i84 < _map83.size; ++_i84)
                  {
                    int _key85;
                    long _val86;
                    _key85 = iprot.readI32();
                    _val86 = iprot.readI64();
                    _val82.put(_key85, _val86);
                  }
                  iprot.readMapEnd();
                }
                this.failed.put(_key81, _val82);
              }
              iprot.readMapEnd();
            }
          } else { 
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 3: // COMPLETE_MS_AVG
          if (field.type == org.apache.thrift.protocol.TType.MAP) {
            {
              org.apache.thrift.protocol.TMap _map87 = iprot.readMapBegin();
              this.complete_ms_avg = new HashMap<String,Map<Integer,Double>>(2*_map87.size);
              for (int _i88 = 0; _i88 < _map87.size; ++_i88)
              {
                String _key89;
                Map<Integer,Double> _val90;
                _key89 = iprot.readString();
                {
                  org.apache.thrift.protocol.TMap _map91 = iprot.readMapBegin();
                  _val90 = new HashMap<Integer,Double>(2*_map91.size);
                  for (int _i92 = 0; _i92 < _map91.size; ++_i92)
                  {
                    int _key93;
                    double _val94;
                    _key93 = iprot.readI32();
                    _val94 = iprot.readDouble();
                    _val90.put(_key93, _val94);
                  }
                  iprot.readMapEnd();
                }
                this.complete_ms_avg.put(_key89, _val90);
              }
              iprot.readMapEnd();
            }
          } else { 
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();
    validate();
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    validate();

    oprot.writeStructBegin(STRUCT_DESC);
    if (this.acked != null) {
      oprot.writeFieldBegin(ACKED_FIELD_DESC);
      {
        oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.MAP, this.acked.size()));
        for (Map.Entry<String, Map<Integer,Long>> _iter95 : this.acked.entrySet())
        {
          oprot.writeString(_iter95.getKey());
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.I32, org.apache.thrift.protocol.TType.I64, _iter95.getValue().size()));
            for (Map.Entry<Integer, Long> _iter96 : _iter95.getValue().entrySet())
            {
              oprot.writeI32(_iter96.getKey());
              oprot.writeI64(_iter96.getValue());
            }
            oprot.writeMapEnd();
          }
        }
        oprot.writeMapEnd();
      }
      oprot.writeFieldEnd();
    }
    if (this.failed != null) {
      oprot.writeFieldBegin(FAILED_FIELD_DESC);
      {
        oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.MAP, this.failed.size()));
        for (Map.Entry<String, Map<Integer,Long>> _iter97 : this.failed.entrySet())
        {
          oprot.writeString(_iter97.getKey());
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.I32, org.apache.thrift.protocol.TType.I64, _iter97.getValue().size()));
            for (Map.Entry<Integer, Long> _iter98 : _iter97.getValue().entrySet())
            {
              oprot.writeI32(_iter98.getKey());
              oprot.writeI64(_iter98.getValue());
            }
            oprot.writeMapEnd();
          }
        }
        oprot.writeMapEnd();
      }
      oprot.writeFieldEnd();
    }
    if (this.complete_ms_avg != null) {
      oprot.writeFieldBegin(COMPLETE_MS_AVG_FIELD_DESC);
      {
        oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.MAP, this.complete_ms_avg.size()));
        for (Map.Entry<String, Map<Integer,Double>> _iter99 : this.complete_ms_avg.entrySet())
        {
          oprot.writeString(_iter99.getKey());
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.I32, org.apache.thrift.protocol.TType.DOUBLE, _iter99.getValue().size()));
            for (Map.Entry<Integer, Double> _iter100 : _iter99.getValue().entrySet())
            {
              oprot.writeI32(_iter100.getKey());
              oprot.writeDouble(_iter100.getValue());
            }
            oprot.writeMapEnd();
          }
        }
        oprot.writeMapEnd();
      }
      oprot.writeFieldEnd();
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("SpoutStats(");
    boolean first = true;

    sb.append("acked:");
    if (this.acked == null) {
      sb.append("null");
    } else {
      sb.append(this.acked);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("failed:");
    if (this.failed == null) {
      sb.append("null");
    } else {
      sb.append(this.failed);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("complete_ms_avg:");
    if (this.complete_ms_avg == null) {
      sb.append("null");
    } else {
      sb.append(this.complete_ms_avg);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!is_set_acked()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'acked' is unset! Struct:" + toString());
    }

    if (!is_set_failed()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'failed' is unset! Struct:" + toString());
    }

    if (!is_set_complete_ms_avg()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'complete_ms_avg' is unset! Struct:" + toString());
    }

  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

}

