package org.roc.configuration;

import java.io.Serializable;

public enum BoltType implements Serializable{
	/**
	 *  对应xml配置文件中的text-hdfs-bolt
	 */
	TEXT_HDFS("com.sugon.roc.hdfs.bolt.TextHdfsSinkBolt","text-hdfs-bolt"),
	/**
	 *  对应xml配置文件中的avro-hdfs-bolt
	 */
	AVRO_HDFS("com.sugon.roc.hdfs.bolt.AvroHdfsSinkBolt","avro-hdfs-bolt"),
	/**
	 *  对应xml配置文件中的hbase-bolt
	 */
	HBASE("com.sugon.roc.hbase.bolt.HBaseSinkBolt","hbase-bolt"),
	/**
	 *  对应xml配置文件中的redis-bolt
	 */
	REDIS("com.sugon.roc.redis.bolt.RedisClusterSinkBolt","redis-bolt"),
	/**
	 *  对应xml配置文件中的customer-bolt
	 */
	CUSTOMER("com.sugon.roc.customer.CustomerETLBolt","customer-bolt");
	
	
	private final String boltClass;
	private final String typeAttribute; // XML tag attribute
	
	private BoltType(String boltClass,String typeAttribute){
		this.boltClass = boltClass;
		this.typeAttribute = typeAttribute;
	}
	
	public String getBoltClass(){
		return boltClass;
	}

	public String getTypeAttribute() {
		return typeAttribute;
	}
	
}
