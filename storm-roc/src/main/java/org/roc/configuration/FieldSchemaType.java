package org.roc.configuration;

import java.io.Serializable;

/**
 * 是数据流的{@link backtype.storm.tuple.Fields}  的各个field的类型
 * @author LC
 *
 */
public enum FieldSchemaType implements Serializable {
	/**
	 * 对应于 {@link com.sugon.roc.hbase.AvroPut}，支持Avro序列化，存放可以快速转化为 {@link org.apache.hadoop.hbase.client.Put}的对象，在Storm各个组件间以其bytes形式进行传输，适用于hbase-bolt，将数据存入hbase
	 */
	AVROPUT("avro-put"),
	/**
	 * 使用者自定义的Avro对象，在Storm各个组件之间传输需要转化为bytes
	 */
	AVRO("avro"), 
	/**
	 * 使用者自定义的kryo对象，使用kryo序列化机制，可以在Storm中利用storm中的register机制实现复杂对象的传输
	 */
	KRYO("kryo"),
	/**
	 * 对应于 {@link com.travelsky.roc.hbase.KryoPut}，支持Kryo序列化，存放可以快速转化为 {@link org.apache.hadoop.hbase.client.Put}的对象，在Storm各个组件间以其bytes形式进行传输，适用于hbase-bolt，将数据存入hbase
	 */
	KRYOPUT("kryo-put"),
	/**
	 * 适用于field是stirng类型的情况
	 */
	STIRNG("string");
	
	private final String typeName; //xml 中的 tag
	private final String avroPutClass = "com.sugon.roc.hbase.AvroPut";
	private final String kryoPutClass = "com.sugon.roc.hbase.KryoPut";
	private static  String [] kryoPutRelatedClasses = {"com.sugon.roc.hbase.KryoPut","java.util.ArrayList","com.sugon.roc.hbase.KryoColumnValue","com.sugon.roc.redis.KryoRedisTuple"};
	
	private FieldSchemaType(String type) {
		this.typeName = type;

	}
	public String getTypeName() {
		return typeName;
	}
	public String getClazzName() {
		if(this.equals(AVROPUT)){
			return avroPutClass;
		}
		else if(this.equals(KRYOPUT)){
			return kryoPutClass;
		}
		else{
			return null;
		}
	}	
	/**
	 * 组件必须要加载的kryo类，即使在配置文件中没有也要加载
	 * @return
	 */
	public static String[]  getKryoPutRelatedClasses(){
		return kryoPutRelatedClasses;
	}
	
}

