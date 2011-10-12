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

public class Grouping extends org.apache.thrift.TUnion<Grouping, Grouping._Fields> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("Grouping");
  private static final org.apache.thrift.protocol.TField FIELDS_FIELD_DESC = new org.apache.thrift.protocol.TField("fields", org.apache.thrift.protocol.TType.LIST, (short)1);
  private static final org.apache.thrift.protocol.TField SHUFFLE_FIELD_DESC = new org.apache.thrift.protocol.TField("shuffle", org.apache.thrift.protocol.TType.STRUCT, (short)2);
  private static final org.apache.thrift.protocol.TField ALL_FIELD_DESC = new org.apache.thrift.protocol.TField("all", org.apache.thrift.protocol.TType.STRUCT, (short)3);
  private static final org.apache.thrift.protocol.TField NONE_FIELD_DESC = new org.apache.thrift.protocol.TField("none", org.apache.thrift.protocol.TType.STRUCT, (short)4);
  private static final org.apache.thrift.protocol.TField DIRECT_FIELD_DESC = new org.apache.thrift.protocol.TField("direct", org.apache.thrift.protocol.TType.STRUCT, (short)5);

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    FIELDS((short)1, "fields"),
    SHUFFLE((short)2, "shuffle"),
    ALL((short)3, "all"),
    NONE((short)4, "none"),
    DIRECT((short)5, "direct");

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
        case 1: // FIELDS
          return FIELDS;
        case 2: // SHUFFLE
          return SHUFFLE;
        case 3: // ALL
          return ALL;
        case 4: // NONE
          return NONE;
        case 5: // DIRECT
          return DIRECT;
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

  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.FIELDS, new org.apache.thrift.meta_data.FieldMetaData("fields", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.SHUFFLE, new org.apache.thrift.meta_data.FieldMetaData("shuffle", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, NullStruct.class)));
    tmpMap.put(_Fields.ALL, new org.apache.thrift.meta_data.FieldMetaData("all", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, NullStruct.class)));
    tmpMap.put(_Fields.NONE, new org.apache.thrift.meta_data.FieldMetaData("none", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, NullStruct.class)));
    tmpMap.put(_Fields.DIRECT, new org.apache.thrift.meta_data.FieldMetaData("direct", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, NullStruct.class)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Grouping.class, metaDataMap);
  }

  public Grouping() {
    super();
  }

  public Grouping(_Fields setField, Object value) {
    super(setField, value);
  }

  public Grouping(Grouping other) {
    super(other);
  }
  public Grouping deepCopy() {
    return new Grouping(this);
  }

  public static Grouping fields(List<String> value) {
    Grouping x = new Grouping();
    x.set_fields(value);
    return x;
  }

  public static Grouping shuffle(NullStruct value) {
    Grouping x = new Grouping();
    x.set_shuffle(value);
    return x;
  }

  public static Grouping all(NullStruct value) {
    Grouping x = new Grouping();
    x.set_all(value);
    return x;
  }

  public static Grouping none(NullStruct value) {
    Grouping x = new Grouping();
    x.set_none(value);
    return x;
  }

  public static Grouping direct(NullStruct value) {
    Grouping x = new Grouping();
    x.set_direct(value);
    return x;
  }


  @Override
  protected void checkType(_Fields setField, Object value) throws ClassCastException {
    switch (setField) {
      case FIELDS:
        if (value instanceof List) {
          break;
        }
        throw new ClassCastException("Was expecting value of type List<String> for field 'fields', but got " + value.getClass().getSimpleName());
      case SHUFFLE:
        if (value instanceof NullStruct) {
          break;
        }
        throw new ClassCastException("Was expecting value of type NullStruct for field 'shuffle', but got " + value.getClass().getSimpleName());
      case ALL:
        if (value instanceof NullStruct) {
          break;
        }
        throw new ClassCastException("Was expecting value of type NullStruct for field 'all', but got " + value.getClass().getSimpleName());
      case NONE:
        if (value instanceof NullStruct) {
          break;
        }
        throw new ClassCastException("Was expecting value of type NullStruct for field 'none', but got " + value.getClass().getSimpleName());
      case DIRECT:
        if (value instanceof NullStruct) {
          break;
        }
        throw new ClassCastException("Was expecting value of type NullStruct for field 'direct', but got " + value.getClass().getSimpleName());
      default:
        throw new IllegalArgumentException("Unknown field id " + setField);
    }
  }

  @Override
  protected Object readValue(org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TField field) throws org.apache.thrift.TException {
    _Fields setField = _Fields.findByThriftId(field.id);
    if (setField != null) {
      switch (setField) {
        case FIELDS:
          if (field.type == FIELDS_FIELD_DESC.type) {
            List<String> fields;
            {
              org.apache.thrift.protocol.TList _list0 = iprot.readListBegin();
              fields = new ArrayList<String>(_list0.size);
              for (int _i1 = 0; _i1 < _list0.size; ++_i1)
              {
                String _elem2;
                _elem2 = iprot.readString();
                fields.add(_elem2);
              }
              iprot.readListEnd();
            }
            return fields;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case SHUFFLE:
          if (field.type == SHUFFLE_FIELD_DESC.type) {
            NullStruct shuffle;
            shuffle = new NullStruct();
            shuffle.read(iprot);
            return shuffle;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case ALL:
          if (field.type == ALL_FIELD_DESC.type) {
            NullStruct all;
            all = new NullStruct();
            all.read(iprot);
            return all;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case NONE:
          if (field.type == NONE_FIELD_DESC.type) {
            NullStruct none;
            none = new NullStruct();
            none.read(iprot);
            return none;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case DIRECT:
          if (field.type == DIRECT_FIELD_DESC.type) {
            NullStruct direct;
            direct = new NullStruct();
            direct.read(iprot);
            return direct;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        default:
          throw new IllegalStateException("setField wasn't null, but didn't match any of the case statements!");
      }
    } else {
      org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
      return null;
    }
  }

  @Override
  protected void writeValue(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    switch (setField_) {
      case FIELDS:
        List<String> fields = (List<String>)value_;
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, fields.size()));
          for (String _iter3 : fields)
          {
            oprot.writeString(_iter3);
          }
          oprot.writeListEnd();
        }
        return;
      case SHUFFLE:
        NullStruct shuffle = (NullStruct)value_;
        shuffle.write(oprot);
        return;
      case ALL:
        NullStruct all = (NullStruct)value_;
        all.write(oprot);
        return;
      case NONE:
        NullStruct none = (NullStruct)value_;
        none.write(oprot);
        return;
      case DIRECT:
        NullStruct direct = (NullStruct)value_;
        direct.write(oprot);
        return;
      default:
        throw new IllegalStateException("Cannot write union with unknown field " + setField_);
    }
  }

  @Override
  protected org.apache.thrift.protocol.TField getFieldDesc(_Fields setField) {
    switch (setField) {
      case FIELDS:
        return FIELDS_FIELD_DESC;
      case SHUFFLE:
        return SHUFFLE_FIELD_DESC;
      case ALL:
        return ALL_FIELD_DESC;
      case NONE:
        return NONE_FIELD_DESC;
      case DIRECT:
        return DIRECT_FIELD_DESC;
      default:
        throw new IllegalArgumentException("Unknown field id " + setField);
    }
  }

  @Override
  protected org.apache.thrift.protocol.TStruct getStructDesc() {
    return STRUCT_DESC;
  }

  @Override
  protected _Fields enumForId(short id) {
    return _Fields.findByThriftIdOrThrow(id);
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }


  public List<String> get_fields() {
    if (getSetField() == _Fields.FIELDS) {
      return (List<String>)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'fields' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void set_fields(List<String> value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.FIELDS;
    value_ = value;
  }

  public NullStruct get_shuffle() {
    if (getSetField() == _Fields.SHUFFLE) {
      return (NullStruct)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'shuffle' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void set_shuffle(NullStruct value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.SHUFFLE;
    value_ = value;
  }

  public NullStruct get_all() {
    if (getSetField() == _Fields.ALL) {
      return (NullStruct)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'all' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void set_all(NullStruct value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.ALL;
    value_ = value;
  }

  public NullStruct get_none() {
    if (getSetField() == _Fields.NONE) {
      return (NullStruct)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'none' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void set_none(NullStruct value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.NONE;
    value_ = value;
  }

  public NullStruct get_direct() {
    if (getSetField() == _Fields.DIRECT) {
      return (NullStruct)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'direct' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void set_direct(NullStruct value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.DIRECT;
    value_ = value;
  }

  public boolean equals(Object other) {
    if (other instanceof Grouping) {
      return equals((Grouping)other);
    } else {
      return false;
    }
  }

  public boolean equals(Grouping other) {
    return other != null && getSetField() == other.getSetField() && getFieldValue().equals(other.getFieldValue());
  }

  @Override
  public int compareTo(Grouping other) {
    int lastComparison = org.apache.thrift.TBaseHelper.compareTo(getSetField(), other.getSetField());
    if (lastComparison == 0) {
      return org.apache.thrift.TBaseHelper.compareTo(getFieldValue(), other.getFieldValue());
    }
    return lastComparison;
  }


  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder();
    hcb.append(this.getClass().getName());
    org.apache.thrift.TFieldIdEnum setField = getSetField();
    if (setField != null) {
      hcb.append(setField.getThriftFieldId());
      Object value = getFieldValue();
      if (value instanceof org.apache.thrift.TEnum) {
        hcb.append(((org.apache.thrift.TEnum)getFieldValue()).getValue());
      } else {
        hcb.append(value);
      }
    }
    return hcb.toHashCode();
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
