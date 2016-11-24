package example.hdfs02;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.tuple.Tuple;
import org.roc.configuration.BoltConfiguration;
import org.roc.configuration.Relation.Dependency;
import org.roc.customer.CustomerETLBolt;
/**
 * 接收string类型的数据，处理后输出String类型数据
 * @author LC
 *
 */
public class SimpleCustomerBolt1 extends CustomerETLBolt {
	private static final long serialVersionUID = 3219026143589748128L;
	public static Logger logger = LoggerFactory.getLogger(SimpleCustomerBolt1.class);
	private String fieldDelimer = null;

	// 需要继承
	public SimpleCustomerBolt1(BoltConfiguration bltConf) {
		super(bltConf);
	}

	@Override
	protected void setup(Map stormConf) {
		List<Dependency> list = this.bltConf.getDependencies();
		if(list.size()>=1){
			// 依赖只有一个
			Dependency dependency = list.get(0);
			// 已经知道是String类型了，在配置文件中schema可以不配置？
			this.fieldDelimer = dependency.getFields().get(0).getTupleFiledDelimer();
			//fieldDelimer =  dependency.getTupleSchema().getClazzName();
			//logger.info("fieldDelimer--"+this.fieldDelimer+"--");
		}
	}
	
	@Override
	public void execute(Tuple input) {
		//can change the process staff
		try {
			Thread.sleep(10000);
			System.out.println("-------------------"+input.toString());
		} catch (InterruptedException e) {
			
		}
		super.execute(input);
		
	}
	

	@Override
	public Object getRecord(Tuple tuple) {
	
		Object obj = tuple.getValue(0);
		String record = null;
		if(obj instanceof String){
			record = (String)obj;
		}
		else if(obj instanceof byte[]){
			record = Bytes.toString((byte[])obj);
		}else{
			logger.warn("Tuple value is not String or bytes");
			this.collector.fail(tuple);
		}
		return record;
	}

	
	
	@Override
	public boolean recordFilter(Object record) {
		// TODO Auto-generated method stub
		return super.recordFilter(record);
	}

	@Override
	public Object fieldSelect(Object record) {
		String [] lines = ((String)record).split(this.fieldDelimer);
		//logger.info("===="+lines.length);
		
		// 只选择第一列,不做规范化
		return lines[0];
	}

}
