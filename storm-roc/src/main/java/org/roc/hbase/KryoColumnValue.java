package org.roc.hbase;

import java.io.Serializable;

import org.roc.exception.HBasePutException;

public  class KryoColumnValue implements Serializable {

	private static final long serialVersionUID = -2841869787921367848L;
	private byte[] family  =null;
	private byte[] qualifier = null;
	private byte[] value = null;
	private long timestamp=0L;
	// need by serialization
	public KryoColumnValue(){}
	public KryoColumnValue(byte[] family, byte[] qualifier, byte[] value, long timestamp){
	    this.family = family;
	    this.qualifier = qualifier;
	    this.value = value;
	    this.timestamp = timestamp;
	}
	public KryoColumnValue(byte[] family, byte[] qualifier, byte[] value){
	    this.family = family;
	    this.qualifier = qualifier;
	    this.value = value;
	}
	
	public java.lang.Object get(int field$) throws HBasePutException {
	    switch (field$) {
	    case 0: return family;
	    case 1: return qualifier;
	    case 2: return value;
	    case 3: return timestamp;
	    default: throw new HBasePutException("ColumnValueWrapper field has bad index");
	    }
	}
	public byte[] getFamily() {
		return family;
	}
	public void setFamily(byte[] family) {
		this.family = family;
	}
	public byte[] getQualifier() {
		return qualifier;
	}
	public void setQualifier(byte[] qualifier) {
		this.qualifier = qualifier;
	}
	public byte[] getValue() {
		return value;
	}
	public void setValue(byte[] value) {
		this.value = value;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
}