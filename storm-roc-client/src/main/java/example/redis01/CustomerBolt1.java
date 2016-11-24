package example.redis01;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.tuple.Tuple;
import org.roc.configuration.BoltConfiguration;
import org.roc.customer.CustomerETLBolt;
import org.roc.redis.KryoRedisTuple;
import org.roc.redis.KryoRedisTuple.ValueType;
import org.roc.redis.KryoRedisTuple.WriteMethod;

/**
 * 
 *  使用河南公安 交通卡口数据测试
 *  主要是测试RedisBolt的使用
 * @author LC
 *
 */
public class CustomerBolt1 extends CustomerETLBolt {

	private static final long serialVersionUID = -1552201336821091363L;

	public static Logger logger = LoggerFactory.getLogger(CustomerBolt1.class);
	private String filedDelimiter = "~";

	// 必须写构造函数，参数是BoltConfiguration的实例
	public CustomerBolt1(BoltConfiguration bltConf) {
		super(bltConf);
	}


	@Override
	public void execute(Tuple input) {
		String[] lines = getRecord(input);
		if (lines != null) {
			try {
				boolean discard = recordFilter(lines);
				// false 不过滤
				if (discard == false) {

					String KKWZBM = lines[2];
					String HPHM = lines[3];

					// 测试字符串类型数据
					// 统计各个卡口通过的车辆数
					KryoRedisTuple redisTuple_STRING = new KryoRedisTuple();
					redisTuple_STRING.setKey(KKWZBM);
					redisTuple_STRING.setMethod(WriteMethod.INCR);
					redisTuple_STRING.setType(ValueType.STRING);
					emitTuple(input, redisTuple_STRING);
					
					// 测试列表类型数据
					// 记录每个车牌通过的卡口
					KryoRedisTuple redisTuple_LIST = new KryoRedisTuple();
					redisTuple_LIST.setKey(HPHM);
					redisTuple_LIST.setValue(KKWZBM);
					redisTuple_LIST.setType(ValueType.LIST);
					redisTuple_LIST.setMethod(WriteMethod.RPUSH);
					emitTuple(input, redisTuple_LIST);
					
					// 测试集合类型数据
					// 收集所有的卡口ID
					KryoRedisTuple redisTuple_SET = new KryoRedisTuple();
					redisTuple_SET.setKey("kkww");
					redisTuple_SET.setValue(KKWZBM);
					redisTuple_SET.setType(ValueType.SET);
					redisTuple_SET.setMethod(WriteMethod.SADD);
					emitTuple(input, redisTuple_SET);
					
					
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				this.collector.ack(input);
			}

		}
	}

	/**
	 * 从tuple中得到记录
	 * 
	 * @param tuple
	 * @return
	 */

	public String[] getRecord(Tuple tuple) {
		Object obj = tuple.getValue(0);
		String[] record = null;
		if (obj instanceof String) {
			record = ((String) obj).split(this.filedDelimiter);
		} else if (obj instanceof byte[]) {
			record = Bytes.toString((byte[]) obj).split(this.filedDelimiter);
			// logger.info(Bytes.toString((byte[]) obj));
		} else {
			logger.warn("Tuple value is not String or bytes");
			this.collector.fail(tuple);
		}
		// logger.info("SUGON " + record.length);
		return record;
	}

	/**
	 * 对记录进行过滤,默认返回false
	 * 
	 * @param record
	 * @return 可以过滤为true，不能过滤掉返回false，
	 */
	public boolean recordFilter(String[] lines) {
		if (lines.length >= 14) {

			if ("".equals(lines[3].trim()) || "".equals(lines[2].trim())) {
				return true;
			}
			return false;

		} else {

			return true;
		}
	}

}