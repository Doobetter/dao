/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package org.roc.hbase;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class AvroPut extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"AvroPut\",\"namespace\":\"com.sugon.roc.hbase\",\"fields\":[{\"name\":\"row\",\"type\":\"bytes\"},{\"name\":\"columnValues\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"AvroColumnValue\",\"fields\":[{\"name\":\"family\",\"type\":\"bytes\"},{\"name\":\"qualifier\",\"type\":\"bytes\"},{\"name\":\"value\",\"type\":\"bytes\"},{\"name\":\"timestamp\",\"type\":[\"long\",\"null\"]}]}}}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.nio.ByteBuffer row;
  @Deprecated public java.util.List<org.roc.hbase.AvroColumnValue> columnValues;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use {@link \#newBuilder()}. 
   */
  public AvroPut() {}

  /**
   * All-args constructor.
   */
  public AvroPut(java.nio.ByteBuffer row, java.util.List<org.roc.hbase.AvroColumnValue> columnValues) {
    this.row = row;
    this.columnValues = columnValues;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return row;
    case 1: return columnValues;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: row = (java.nio.ByteBuffer)value$; break;
    case 1: columnValues = (java.util.List<org.roc.hbase.AvroColumnValue>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'row' field.
   */
  public java.nio.ByteBuffer getRow() {
    return row;
  }

  /**
   * Sets the value of the 'row' field.
   * @param value the value to set.
   */
  public void setRow(java.nio.ByteBuffer value) {
    this.row = value;
  }

  /**
   * Gets the value of the 'columnValues' field.
   */
  public java.util.List<org.roc.hbase.AvroColumnValue> getColumnValues() {
    return columnValues;
  }

  /**
   * Sets the value of the 'columnValues' field.
   * @param value the value to set.
   */
  public void setColumnValues(java.util.List<org.roc.hbase.AvroColumnValue> value) {
    this.columnValues = value;
  }

  /** Creates a new AvroPut RecordBuilder */
  public static org.roc.hbase.AvroPut.Builder newBuilder() {
    return new org.roc.hbase.AvroPut.Builder();
  }
  
  /** Creates a new AvroPut RecordBuilder by copying an existing Builder */
  public static org.roc.hbase.AvroPut.Builder newBuilder(org.roc.hbase.AvroPut.Builder other) {
    return new org.roc.hbase.AvroPut.Builder(other);
  }
  
  /** Creates a new AvroPut RecordBuilder by copying an existing AvroPut instance */
  public static org.roc.hbase.AvroPut.Builder newBuilder(org.roc.hbase.AvroPut other) {
    return new org.roc.hbase.AvroPut.Builder(other);
  }
  
  /**
   * RecordBuilder for AvroPut instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<AvroPut>
    implements org.apache.avro.data.RecordBuilder<AvroPut> {

    private java.nio.ByteBuffer row;
    private java.util.List<org.roc.hbase.AvroColumnValue> columnValues;

    /** Creates a new Builder */
    private Builder() {
      super(org.roc.hbase.AvroPut.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(org.roc.hbase.AvroPut.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.row)) {
        this.row = data().deepCopy(fields()[0].schema(), other.row);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.columnValues)) {
        this.columnValues = data().deepCopy(fields()[1].schema(), other.columnValues);
        fieldSetFlags()[1] = true;
      }
    }
    
    /** Creates a Builder by copying an existing AvroPut instance */
    private Builder(org.roc.hbase.AvroPut other) {
            super(org.roc.hbase.AvroPut.SCHEMA$);
      if (isValidValue(fields()[0], other.row)) {
        this.row = data().deepCopy(fields()[0].schema(), other.row);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.columnValues)) {
        this.columnValues = data().deepCopy(fields()[1].schema(), other.columnValues);
        fieldSetFlags()[1] = true;
      }
    }

    /** Gets the value of the 'row' field */
    public java.nio.ByteBuffer getRow() {
      return row;
    }
    
    /** Sets the value of the 'row' field */
    public org.roc.hbase.AvroPut.Builder setRow(java.nio.ByteBuffer value) {
      validate(fields()[0], value);
      this.row = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'row' field has been set */
    public boolean hasRow() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'row' field */
    public org.roc.hbase.AvroPut.Builder clearRow() {
      row = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'columnValues' field */
    public java.util.List<org.roc.hbase.AvroColumnValue> getColumnValues() {
      return columnValues;
    }
    
    /** Sets the value of the 'columnValues' field */
    public org.roc.hbase.AvroPut.Builder setColumnValues(java.util.List<org.roc.hbase.AvroColumnValue> value) {
      validate(fields()[1], value);
      this.columnValues = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'columnValues' field has been set */
    public boolean hasColumnValues() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'columnValues' field */
    public org.roc.hbase.AvroPut.Builder clearColumnValues() {
      columnValues = null;
      fieldSetFlags()[1] = false;
      return this;
    }

   // @Override
    public AvroPut build() {
      try {
        AvroPut record = new AvroPut();
        record.row = fieldSetFlags()[0] ? this.row : (java.nio.ByteBuffer) defaultValue(fields()[0]);
        record.columnValues = fieldSetFlags()[1] ? this.columnValues : (java.util.List<org.roc.hbase.AvroColumnValue>) defaultValue(fields()[1]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
