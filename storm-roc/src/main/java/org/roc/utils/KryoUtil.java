package org.roc.utils;

import java.util.ArrayList;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.roc.hbase.KryoColumnValue;
import org.roc.hbase.KryoPut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Kryo 序列化工具类
 * 主要是用在接收Kafka中的Avro的数据
 * @author LC
 *
 */
public class KryoUtil implements ISerializationUtil {
	public static Logger logger = LoggerFactory.getLogger(KryoUtil.class);
	private Kryo kryo = null;
	private Output output = null;
	public KryoUtil(){
		kryo = new Kryo();
		output = new Output(300,-1);
	}
	/**
	 *  用将要序列化的类初始化KryoUtil
	 * @param cls
	 */
	public  KryoUtil(Class<?> ... cls){
		kryo = new Kryo();
		output = new Output(300,-1);
		for (Class<?> c : cls){
			this.kryo.register(c);
		}
	}
	public <T> void register(Class<T> clazz){
		this.kryo.register(clazz);
	}
	
	
	/**
	 *  几乎在storm中用不到，可以再Kafka的生产者里使用
	 */
	@Override
	public <T> byte[] toBytes(T t) {
		output.clear();
		kryo.writeObject(output, t);
		output.flush();
		byte[] bs = output.toBytes();
		return bs;
	}
	/**
	 * 如果接收的数据是Kryo序列化的字节数组，需要先转化为Kryo对象，然后做相应的处理
	 */
	@Override
	public  <T> T toObject(byte[] bs, Class<T> clazz) {
		Input input = new Input(bs);
		T someObject = kryo.readObject(input, clazz);
		input.close();
		return someObject;
	}

	/**
	 * 只有KryoPut类型的可以转化
	 */
	@Override
	public <T> Put toPut(T t) {
		KryoPut kryoPut = (KryoPut)t;
 		if(kryoPut.getRowkey() == null){
			logger.error("PutWrapper object's field rowkey is null ");
			return null;
		}
		Put put = new Put(kryoPut.getRowkey());
		ArrayList<KryoColumnValue> list  = kryoPut.getColumnValues();
		for (KryoColumnValue column : list){
			put.add(column.getFamily(),column.getQualifier(),column.getValue());
		}
		return put;
	}

	public void colse(){
		this.output.close();
	}
	
	public static void main(String[] args) {
		KryoUtil util = new KryoUtil(KryoPut.class,KryoColumnValue.class,ArrayList.class);
		/*util.register(KryoPut.class);
		util.register(ColumnValueWrapper.class);
		util.register(ArrayList.class);*/
		Put put = new Put(Bytes.toBytes("001"));
		put.add(Bytes.toBytes("a"), Bytes.toBytes("name"), Bytes.toBytes("liucheng"));
		KryoPut w1= new KryoPut(put);
		System.out.println(Bytes.toString(util.toBytes(w1)));
	
		
		Put put1 = new Put(Bytes.toBytes("002"));
		put.add(Bytes.toBytes("a"), Bytes.toBytes("name"), Bytes.toBytes("liucheng"));
		KryoPut w2= new KryoPut(put1);
		
		
		KryoPut w3 = util.toObject(util.toBytes(w2),KryoPut.class);
		System.out.println(Bytes.toString(w3.getRowkey()));
	}

	
}
