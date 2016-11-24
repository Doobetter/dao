package kafka;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {

	// 生成 String类型的数据
	public static void produceStringData(){
		Random rnd = new Random();

		Properties props = new Properties();
		props.put("metadata.broker.list", "10.0.31.188:9092,10.0.31.189:9092,10.0.31.199:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "kafka.SimplePartitioner");
		props.put("request.required.acks", "1");
		 
		ProducerConfig config = new ProducerConfig(props);

		Producer<String, String> producer = new Producer<String, String>(config);
		//kafka.api.OffsetRequest.
		int count=0;
		while(true){
			long runtime = new Date().getTime();
			String ip = rnd.nextInt(255)+"."+rnd.nextInt(255)+"."+rnd.nextInt(255)+"."+rnd.nextInt(255);
			String msg = runtime + "\001" + ip;
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(
					":q", ip, msg);
			producer.send(data);
			if (count++ % 10000==0){
			System.out.println(msg);}
			// try {
			// Thread.sleep(10);
			// } catch (InterruptedException e) {
			// producer.close();
			// }
		}
	}
	
	public static void main(String[] args) {
		produceStringData();
	}

}
