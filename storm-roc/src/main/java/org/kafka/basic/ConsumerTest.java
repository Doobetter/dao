package org.kafka.basic;

import java.io.File;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.roc.utils.ConfigurationUtils;


public class ConsumerTest {

	public static void main(String[] args) {
		
		System.out.println(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);;
		// KafkaSpoutConfig.Consumer.ENABLE_AUTO_COMMIT;
		// Consumer consumerThread = new Consumer(KafkaProperties.TOPIC);
		// consumerThread.start();
		String fromStart = null;
		if(args.length>1){
			fromStart = args[0];
		}
		
		
		String sslConfPath = ConfigurationUtils.getAbsolutePath("conf") + File.separator + "ssl";
		System.out.println(sslConfPath);

		Properties props = new Properties();
		props.put("bootstrap.servers", "10.0.33.128:9093,10.0.33.129:9093");
		props.put("group.id", "consume-group-1");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		if("1".equals(fromStart)){
			props.put("auto.offset.reset", "earliest");
		}

		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("security.protocol", "SSL");
		props.put("ssl.truststore.location", sslConfPath + File.separator + "client.truststore.jks");
		props.put("ssl.truststore.password", "xdata123");
		props.put("ssl.keystore.location", sslConfPath + File.separator + "server.keystore.jks");
		props.put("ssl.keystore.password", "123456");
		props.put("ssl.key.password", "123456");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList("testtopic"));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(10);
			System.out.println(records.isEmpty());
			for (ConsumerRecord<String, String> record : records)
				System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(),
						record.value());
		}

	}

}
