package example.hdfs03;

import java.util.Map;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.roc.configuration.BoltConfiguration;
import org.roc.customer.CustomerETLBolt;
import org.roc.hbase.AvroPut;
import org.roc.utils.AvroUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 接收string类型的数据，处理后输出String类型数据
 * @author LC
 *
 */
public class AvroCustomerBolt1 extends CustomerETLBolt {
	public AvroCustomerBolt1(BoltConfiguration bltConf) {
		super(bltConf);
	}

	public static Logger logger = LoggerFactory.getLogger(AvroCustomerBolt1.class);
	//Important！！ 这个类中的变量都要在setup中初始化
	private AvroUtil avroUtil =null;

	
	@Override
	protected void setup(Map stormConf) {
		avroUtil = new AvroUtil(AvroPut.class);
	}

	@Override
	public void execute(Tuple input) {
		//can change the process staff
		super.execute(input);
	}

	@Override
	public Object getRecord(Tuple tuple) {		
		byte [] bytes = tuple.getBinary(0);
		return avroUtil.toObject(bytes, AvroPut.class);
	}
	
	@Override
	public void emitTuple(Tuple anchor, Object record) {
		this.collector.emit(anchor,new Values(avroUtil.toBytes(record)));
	}


}
