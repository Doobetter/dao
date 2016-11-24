package org.roc.kafka.spout;


import java.util.Properties;

import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.roc.configuration.SpoutConfiguration;
import org.roc.configuration.SpoutConfiguration.SpoutConfConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 封装KafkaSpout，用于topology的构建过程
 * @author LC
 *
 */
public class KafkaSpoutBuilder {
	public static Logger logger = LoggerFactory.getLogger(KafkaSpoutBuilder.class);
	
	SpoutConfig kafkaConfig = null;

	public KafkaSpoutBuilder(SpoutConfiguration spoutConf) {
		Properties props = spoutConf.getProperties();
		String value = null;
		String  zkSpoutId = spoutConf.getBelongTopologyID();
		ZkHosts zkHosts = new ZkHosts(props.getProperty(SpoutConfConstant.KAFKA_ZOOKEEPER_HOSTS));
		String zkRoot = props.getProperty(SpoutConfConstant.KAFKA_ZKROOT);
		String topic = props.getProperty(SpoutConfConstant.KAFKA_TOPIC);
		long maxOffsetBehind = Long.parseLong(((value=props.getProperty(SpoutConfConstant.KAFKA_MAXOFFSETBEHIND))!=null? value : "-1" ));
		Boolean isKafkaForceFromStart = Boolean.parseBoolean((value=props.getProperty(SpoutConfConstant.KAFKA_ISFORCEFROMSTART)) != null ? value : "false");
		kafkaConfig = new SpoutConfig(zkHosts, topic, zkRoot,zkSpoutId);
		if(maxOffsetBehind != -1){
			if ( maxOffsetBehind < 100000000) {
				// 默认防止丢数据
				maxOffsetBehind = Long.MAX_VALUE;
			}
			kafkaConfig.maxOffsetBehind = maxOffsetBehind;
		}else{
			// use default
		}
		
		//kafkaConfig.forceFromStart = isKafkaForceFromStart;
		kafkaConfig.ignoreZkOffsets = isKafkaForceFromStart;
		logger.info(zkSpoutId);
		//logger.info(zkHosts);
		logger.info(zkRoot);
		logger.info(topic);
	
	}
	
	public KafkaSpout build(){
		return new KafkaSpout(kafkaConfig);
	}

}
