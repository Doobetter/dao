package org.roc.customer;


import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.roc.configuration.BoltConfiguration;
import org.roc.configuration.Relation.Emit;
import org.roc.exception.ConfigurationException;
/**
 * 自定义Bolt的抽象类，使用者自定义的Bolt可以继承CustomerETLBolt
 * 该类也可以作为一个模板类，使用者仿照此类继承IRichBolt 来实现更为复杂的业务
 * @author LC
 *
 */
public abstract class CustomerETLBolt implements IRichBolt {
	private static final long serialVersionUID = 1288761315608348783L;
	public static Logger logger = LoggerFactory.getLogger(CustomerETLBolt.class);
	protected OutputCollector collector = null;
	protected BoltConfiguration bltConf = null;
	protected String bltConfJSON = null; 

	public CustomerETLBolt(){}
	
	public CustomerETLBolt(BoltConfiguration bltConf){
		this.bltConf = bltConf;
		this.bltConfJSON = this.bltConf.toJSON();
	}
	
	/**
	 * 获取参数,从bltConf中得到基础类型、可序列化的变量
	 */
	protected  void  setup(Map stormConf){
		
	}
	
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.bltConf = BoltConfiguration.getFromJSON(this.bltConfJSON);
		setup(stormConf);
	}
	
	/**
	 *  按照 过滤 选择 的ETL过程 编写
	 */
	@Override
	public void execute(Tuple input) {
		Object record = getRecord(input);
		if(record!=null){
			try{
				boolean discard = recordFilter(record);
				// false 不过滤
				if(discard == false){
					// select,默认选择整个对象 
					Object record1 = fieldSelect(record);
					// emit
					emitTuple(input,record1);
				}	
				
				this.collector.ack(input);
				
			}
			catch(Exception e){
				this.collector.fail(input);
				logger.error("Process of ETL has something wrong", e);;
			}
			
		}else{
			this.collector.ack(input);
		}
	}

	/**
	 * 从tuple中得到记录
	 * @param tuple
	 * @return
	 */
	public abstract Object getRecord(Tuple tuple);
	/**
	 * 对记录进行过滤,默认返回false
	 * @param record
	 * @return 可以过滤为true，不能过滤掉返回false，
	 */
	public boolean recordFilter(Object record){
		return false;
	}

	/**
	 * 选择记录的某些字段，对字段值进行规范化，组成新的对象。默认返回当前对象
	 * @param record
	 * @return
	 */
	public Object fieldSelect(Object record){
		return record;
	}
	

	
	/**
	 * Emit Tuples
	 * @param anchor
	 * @param output
	 */
	public void emitTuple(Tuple anchor, Object record){
		
		this.collector.emit(anchor, new Values(record));

	}
	public void emitTuple(Tuple anchor,String streamId, Object... record){
		this.collector.emit(streamId, anchor, new Values(record));
	}

	public void emit(Map<String,Values> emits){

	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
	/**
	 * this implement is a simple example 通过解析配置信息来declare Output Fields
	 * 大多数情况可直接copy使用或者继承使用
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		if(this.bltConf!=null){
			List<Emit> list = this.bltConf.getEmits();
			//多个emit时
			if(list.size()>1){
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
			//一个emit时
			else if (list.size()==1){
				Emit emit = list.get(0);
				String streamId = emit.getStreamID();
				logger.info("streamId--"+streamId);
				List<String> fields = emit.getEmitFields();
				if( streamId != null ){
					declarer.declareStream(streamId, new Fields(fields));
				}else{
					declarer.declare(new Fields(fields));
				}	
			}
			//无emit配置参数时
			else{
				// default,防止配置文件中不写Emit参数
				logger.warn("emit参数没有指定，默认指定var1为field名");
				declarer.declare(new Fields("var1"));
			}
		}else{
			throw new ConfigurationException("bltConf is null !");
		}
		
	}


	@Override
	public void cleanup() {
		// do nothing
	}
	
}
