package example.hdfs02;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.roc.configuration.BoltConfiguration;
import org.roc.configuration.ConfigurationUtils;
import org.roc.configuration.Relation.Emit;
import org.roc.exception.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 直接实现IRichBolt，实现更复杂的业务处理
 * 本例：接收string类型的数据，简单处理后输出String类型数据
 * 
 * @author LC
 *
 */
public class SimpleCustomerBolt2 implements IRichBolt {
	public static Logger logger = LoggerFactory.getLogger(SimpleCustomerBolt2.class);
	
	protected OutputCollector collector = null;
	protected BoltConfiguration bltConf = null;
	protected String bltConfJSON = null;

	// 构造函数
	public SimpleCustomerBolt2(BoltConfiguration bltConf) {
		this.bltConf = bltConf;
		this.bltConfJSON = this.bltConf.toJSON();
	}



	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.bltConf = BoltConfiguration.getFromJSON(this.bltConfJSON);
		setup(stormConf);
	}

	/**
	 * 获取参数,从bltConf中得到基础类型、可序列化的变量
	 */
	protected void setup(Map stormConf) {

	}
	
	public void execute(Tuple input) {
		Object record = getRecord(input);
		if (record != null) {
			try {
				boolean discard = recordFilter(record);
				// false 不过滤
				if (discard == false) {
					// select
					Object record1 = fieldSelect(record);
					// emit
					emitTuple(input, record1);
					// ack
					this.collector.ack(input);

				} else {
					this.collector.ack(input);
				}
			} catch (Exception e) {
				this.collector.fail(input);
				e.printStackTrace();
			}

		}
	}

	/**
	 * 从tuple中得到记录
	 * 
	 * @param tuple
	 * @return
	 */

	public Object getRecord(Tuple tuple) {
		// TODO Auto-generated method stub

		Object obj = tuple.getValue(0);
		String record = null;
		if (obj instanceof String) {
			record = (String) obj;
		} else if (obj instanceof byte[]) {
			record = Bytes.toString((byte[]) obj);
		} else {
			logger.warn("Tuple value is not String or bytes");
			this.collector.fail(tuple);
		}
		return record;
	}

	/**
	 * 对记录进行过滤,默认返回false
	 * 
	 * @param record
	 * @return 可以过滤为true，不能过滤掉返回false，
	 */
	public boolean recordFilter(Object record) {
		return false;
	}

	/**
	 * 选择记录的某些字段，对字段值进行规范化，组成新的对象。默认返回当前对象
	 * 
	 * @param record
	 * @return
	 */
	public Object fieldSelect(Object record) {
		return record;
	}

	/**
	 * Emit Tuples
	 * 
	 * @param anchor
	 * @param output
	 */
	public void emitTuple(Tuple anchor, Object record) {

		this.collector.emit(anchor, new Values(record));

	}


	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.putAll(ConfigurationUtils.propsToMap(this.bltConf.getProperties()));
		return conf;

	}

	/**
	 * this implement is a simple example
	 */

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		if(this.bltConf!=null){
			List<Emit> list = this.bltConf.getEmits();
			if(list.size()>0){
				for (Emit emit : list ){
					String streamId = emit.getStreamID();
					List<String> fields = emit.getEmitFields();
					if( streamId != null ){
						declarer.declareStream(streamId, new Fields(fields));
					}else{
						throw new ConfigurationException("当有多个emit时，对每个emit必须指定streamID");
					}	
				}
			}
			else{
				// default,防止配置文件中不写Emit参数
				logger.info("emit参数没有指定，默认指定var1为field名");
				declarer.declare(new Fields("var1"));
			}
		}

	}


	public void cleanup() {
		// do nothing
	}

}
