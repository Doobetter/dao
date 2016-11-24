package example.hdfs04;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class NormalAvroKafkaProducer {

	// 生成 String类型的数据
	public static void produceStringData(int startFlag, int limitNumber){
		int limit = limitNumber + startFlag ;
		Properties props = new Properties();
		props.put("metadata.broker.list", "10.0.31.188:9092,10.0.31.189:9092,10.0.31.190:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "kafka.SimplePartitioner");
		props.put("request.required.acks", "1");
		 
		ProducerConfig config = new ProducerConfig(props);

		Producer<String, String> producer = new Producer<String, String>(config);
		int count=startFlag;
		while(true){

			String user_str = "name_"+count +","+"color_"+count +","+count;

			KeyedMessage<String, String> data = new KeyedMessage<String, String>(
					"test1", user_str);
			producer.send(data);
			if (count++ >= limit){
				System.out.println(user_str);
				break;
				
			}
		}
	}
	
	public static void main(String[] args) {
		produceStringData(19,2);
	}

}
