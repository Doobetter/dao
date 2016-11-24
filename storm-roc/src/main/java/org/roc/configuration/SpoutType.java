package org.roc.configuration;

import java.io.Serializable;

public enum SpoutType implements Serializable {
	KAFKA("com.sugon.roc.kafka.spout.KafkaSpoutBuilder","kafka-spout"),
	/**
	 * 2016-07-28 for new kafka client api
	 */
	KAFKA_CLIENT("com.sugon.roc.kafka.client.spout.KafkaClientSpoutBuilder","kafka-client-spout"),
	TEXT_HDFS("com.sugon.roc.hdfs.spout.TextHdfsSpout","text-hdfs-spout"),
	AVRO_HDFS("com.sugon.roc.hdfs.spout.AvroHdfsSpout","avro-hdfs-spout");
	private final String spoutClass;
	private final String typeAttribute; // xml tag
	private SpoutType(String spoutClass,String typeAttribute){
		this.spoutClass = spoutClass;
		this.typeAttribute = typeAttribute;
	}
	public String getSpoutClass(){
		return spoutClass;
	}
	public String getTypeAttribute() {
		return typeAttribute;
	}
}
