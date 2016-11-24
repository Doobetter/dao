package org.roc.configuration;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpoutConfiguration {
	public static Logger logger = LoggerFactory.getLogger(ConfigurationMain.class);
	private String belongTopologyID = "TOPOLOGY_" + System.currentTimeMillis();

	private String id = "SPOUT_" + System.currentTimeMillis();

	private SpoutType type = null;

	private Properties properties = null;

	public SpoutConfiguration() {
	}

	public SpoutConfiguration(String belongTopologyID) {
		this.belongTopologyID = belongTopologyID;
	}

	public Object getConf(String key) {
		if (this.properties == null) {
			logger.error("Porperties is null Please init firstly");
			return null;
		}
		return this.properties.get(key);
	}

	public void setType(String type) {
		// 查找！
		SpoutType[] types = SpoutType.class.getEnumConstants();
		for (SpoutType t : types) {
			if (type.equals(t.getTypeAttribute())) {
				this.type = t;
				break;
			}
		}
	}

	public String getBelongTopologyID() {
		return belongTopologyID;
	}

	public void setBelongTopologyID(String belongTopologyID) {
		this.belongTopologyID = belongTopologyID;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public SpoutType getType() {
		return type;
	}

	public void setType(SpoutType type) {
		this.type = type;
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	public static void main(String[] args) {
		System.out.println(SpoutType.KAFKA.getSpoutClass());
	}

	public static class SpoutConfConstant {

		public static final String SPOUT_EXECUTORNUMBER = "spout.executorNumber";

		// Properties' Names For Kafka Spout
		public static final String KAFKA_TOPIC = "kafka.topic";
		public static final String KAFKA_ZOOKEEPER_HOSTS = "kafka.zookeeper.hosts";
		public static final String KAFKA_ZKROOT = "kafka.zkRoot";
		public static final String KAFKA_ISFORCEFROMSTART = "kafka.isForceFromStart";
		public static final String KAFKA_MAXOFFSETBEHIND = "kafka.maxOffsetBehind";

		// props for kafka-client-spout
		
		
		
		
		// hdfs
		public static final String HADOOP_CONF_PATH = "hadoop.conf.path";
		public static final String HDFS_INPUT_PATH = "hdfs.input.path";
		public static final String HDFS_USERNAME = "hdfs.userName";
		public static final String HDFS_ZOOKEEPER_HOSTS = "hdfs.zookeeper.hosts";
		public static final String HDFS_ZKROOT = "hdfs.zkRoot";
		public static final String HDFS_TEXT_DELIMER = "hdfs.text.delimer";
		public static final String HDFS_AVRO_SCHEMA = "hdfs.avro.schema";
		public static final String HDFS_ZK_COMMITINTERVAL_TUPLE = "hdfs.zk.commitInterval.tuple";

	}

}
