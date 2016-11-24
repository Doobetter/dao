package org.kafka.basic;

import java.io.File;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.roc.configuration.ConfigurationUtils;

public class ProducerTest {

	public static void main(String[] args) throws InterruptedException {
		
		String sslConfPath = ConfigurationUtils.getAbsolutePath("conf") + File.separator + "ssl";
		System.out.println(sslConfPath);
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "10.0.33.129:9093,10.0.33.128:9093");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("security.protocol", "SSL");
		props.put("ssl.truststore.location", sslConfPath + File.separator + "client.truststore.jks");
		props.put("ssl.truststore.password", "xdata123");
		props.put("ssl.keystore.location", sslConfPath + File.separator + "server.keystore.jks");
		props.put("ssl.keystore.password", "xdata123");
		props.put("ssl.key.password", "xdata123");
		System.out.println("11111111");
		Producer<String, String> producer = new KafkaProducer<>(props);
		System.out.println("2222222");
		for (int i = 0; i < 100000; i++) {
			System.out.println(Integer.toString(i));
			//Thread.sleep(10000);
			producer.send(new ProducerRecord<String, String>("testtopic", Integer.toString(i), Integer.toString(i)));
		}
		producer.close();
	}

}
