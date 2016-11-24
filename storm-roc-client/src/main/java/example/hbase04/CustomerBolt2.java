package example.hbase04;

import java.util.Map;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.roc.configuration.BoltConfiguration;
import org.roc.customer.CustomerETLBolt;
import org.roc.hbase.KryoPut;
import org.roc.utils.KryoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 接收KryoPut的bytes类型的数据，解析为kryoPut对象，利用Storm使用的Kryo序列化传输到下一个节点
 * @author LC
 *
 */
public class CustomerBolt2 extends CustomerETLBolt {
	public CustomerBolt2(BoltConfiguration bltConf) {
		super(bltConf);
	}

	public static Logger logger = LoggerFactory.getLogger(CustomerBolt2.class);
	//Important！！ 这个类中的变量都要在setup中初始化
	//private KryoUtil kryoUtil =null;

	
	@Override
	protected void setup(Map stormConf) {
		
	}

/*	@Override
	public void execute(Tuple input) {
		//can change the process staff
		super.execute(input);
	}*/

	@Override
	public Object getRecord(Tuple tuple) {		
		return  tuple.getValue(0);
	}
	
	@Override
	public void emitTuple(Tuple anchor, Object record) {
		this.collector.emit(anchor,new Values(record));
	}


}
