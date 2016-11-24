package org.roc.kafka.client.spout;


import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutStreams;
import org.apache.storm.kafka.spout.KafkaSpoutTuplesBuilder;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.roc.configuration.SpoutConfiguration;
import org.roc.configuration.SpoutConfiguration.SpoutConfConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 封装KafkaSpout，用于topology的构建过程
 * @author LC
 *
 */
public class KafkaClientSpoutBuilder {
	public static Logger logger = LoggerFactory.getLogger(KafkaClientSpoutBuilder.class);

	KafkaSpoutConfig<String, String> ksc = null;
	
	private boolean checkProps(Properties props){
		// Some properties must be config
		String value = null;
		if(props.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)==null){
			
		}
		
		if((value=props.getProperty("ssl.security"))== null ? false : Boolean.parseBoolean(value) == true){
			// need ssl security
			
		}else{
			
		}
		
		return true;
	}
	
	public KafkaClientSpoutBuilder(SpoutConfiguration spoutConf) {
		Properties props = spoutConf.getProperties();
		
		
		System.out.println(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
		String value = null;
		String  zkSpoutId = spoutConf.getBelongTopologyID();
		String zkRoot = props.getProperty(SpoutConfConstant.KAFKA_ZKROOT);
		String topic = props.getProperty(SpoutConfConstant.KAFKA_TOPIC);
		
		// 构建 KafkaSpoutConfig
		
		
		
	
	}
	
	public KafkaSpout build(){
		return new KafkaSpout(ksc);
	}

}
