package org.roc.hbase.bolt;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.roc.configuration.BoltConfiguration;
import org.roc.configuration.FieldSchemaType;
import org.roc.configuration.BoltConfiguration.BoltConfConstant;
import org.roc.configuration.Relation.Dependency;
import org.roc.configuration.Relation.Emit;
import org.roc.exception.ConfigurationException;
import org.roc.hbase.AvroPut;
import org.roc.hbase.KryoColumnValue;
import org.roc.hbase.KryoPut;
import org.roc.hbase.ManualFlushHTable;
import org.roc.hbase.utils.HBaseUtils;
import org.roc.utils.AvroUtil;
import org.roc.utils.ISerializationUtil;
import org.roc.utils.KryoUtil;

/**
 * HBase Bolt 将数据存入HBase <br>
 * 接收两种类型的数据，实际上是三种<br>
 * 1.{@link org.roc.hbase.AvroPut}的字节数组<br>
 * 2.{@link org.roc.hbase.KryoPut}的对象<br>
 * 3.{@link org.roc.hbase.KryoPut}的字节数组<br>
 * ISerializationUtil 是根据tupleSchemaType实例化的<br>
 * @author liucheng
 * 
 */
public class HBaseSinkBolt implements IRichBolt {

	private static final long serialVersionUID = 5586032199951576789L;

	public static Logger logger = LoggerFactory.getLogger(HBaseSinkBolt.class);

	private OutputCollector collector = null;

	private BoltConfiguration boltConf;
	private String confJSON;
	private String tableName;
	private long writeBufferSize = 4 * 1024 * 1024;
	private int manualFlushTupleInterval = 700;
	private boolean isHBaseFailedHandlerON = false;
	private long hbaseStatusCheckInterval = 1 * 60 * 1000;
	private String hadoopConfPath = null;;

	private HashSet<String> dependencyStreamIDSet = new HashSet<String>();

	private FieldSchemaType tupleSchemaType;
	private Class<?> tupleSchemaClazz;
	ISerializationUtil serializationUtil = null;

	// hbase table operator
	private ManualFlushHTable hTable;
	private Configuration hBaseConf = null;

	private boolean hbaseAvailable = true;
	private long hbaseLastFailedTime = Long.MIN_VALUE;

	private ArrayList<Tuple> tupleList = null;
	private int hbaseErrorCount = 0;

	// end 参数设置函数

	public HBaseSinkBolt() {
	}

	public HBaseSinkBolt(BoltConfiguration boltConf) {
		this.boltConf = boltConf;
		this.settingByConfig();
	}

	private void settingByConfig() {
		Properties props = this.boltConf.getProperties();
		this.hadoopConfPath = props.getProperty(BoltConfConstant.HADOOP_CONF_PATH);
		this.tableName = props.getProperty(BoltConfConstant.HBASE_TABLENAME);
		this.writeBufferSize = Long.parseLong(props.getProperty(BoltConfConstant.HBASE_WRITEBUFFERSIZE));
		this.manualFlushTupleInterval = Integer.parseInt(props.getProperty(BoltConfConstant.HBASE_MANUALFLUSHLIMIT));
		this.isHBaseFailedHandlerON = Boolean.parseBoolean(props.getProperty(BoltConfConstant.HBASE_IS_HBASE_FAILED_HANDLER_ON));
		this.hbaseStatusCheckInterval = Long.parseLong(props.getProperty(BoltConfConstant.HBASE_STATUS_CHECK_INTERVAL));

		confJSON = this.boltConf.toJSON();

		logger.info("----" + confJSON);
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

		logger.info("Preparing HBase Bolt...");
		this.collector = collector;

		this.boltConf = BoltConfiguration.getFromJSON(this.confJSON);

		// 输入依赖只有一个
		ArrayList<Dependency> dependencyList = this.boltConf.getDependencies();
		if (dependencyList.size() == 1) {
			this.dependencyStreamIDSet.add(dependencyList.get(0).getStreamID());
			this.tupleSchemaType = dependencyList.get(0).getFields().get(0).getType();
			if (this.tupleSchemaType == FieldSchemaType.AVROPUT) {
				this.serializationUtil = new AvroUtil(AvroPut.class);
				tupleSchemaClazz = AvroPut.class;
			} else if (this.tupleSchemaType == FieldSchemaType.KRYOPUT) {
				this.serializationUtil = new KryoUtil(KryoPut.class, KryoColumnValue.class, ArrayList.class);
				tupleSchemaClazz = KryoPut.class;
			}
		} else {
			throw new ConfigurationException("Only one Dependency be permitted! Check your conf file for compenentID "+ this.boltConf.getId());
		}

		tupleList = null;
		tupleList = new ArrayList<Tuple>(this.manualFlushTupleInterval);

		logger.info("load hbase-site.xml hdfs-site.xml core-site.xml");
		
		//到storm/hadoop-conf/下读取hbase-site.xml,生成Configuration对象，要保证hbase-site.xml 、hdfs-site.xml、core-site.xml存在
		// 加载hbase配置文件
		Path hbase_conf = new Path(hadoopConfPath + File.separator + "hbase-site.xml");
		Path hdfs_conf = new Path(hadoopConfPath + File.separator + "hdfs-site.xml");
		Path core_conf = new Path(hadoopConfPath + File.separator + "core-site.xml");
		hBaseConf = HBaseConfiguration.create(); // 加载默认的配置
		// 覆盖配置
		hBaseConf.addResource(core_conf);
		hBaseConf.addResource(hdfs_conf);
		hBaseConf.addResource(hbase_conf);
		// default 10
		hBaseConf.setInt("hbase.client.retries.number", 1);
		// default 1000 , 1s
		hBaseConf.setLong("hbase.client.pause", 300);

		Configuration conf = hBaseConf;

		logger.info("[TEST] Get key-value from the hbase-site.xml ( hbase.rootdir=" + conf.get("hbase.rootdir") + "  )");

		
		try {
			this.hTable = new ManualFlushHTable(conf, this.tableName);
			this.hTable.setAutoFlush(false, false);
			this.hTable.setWriteBufferSize(this.writeBufferSize);
		} catch (IOException e) {
			logger.error("Exception in prepare, check HBase's configuration ",e);
		}

	}

	/**
	 * 必须使用修改后的kafka-spout，使得messageId的toString方法包括了paritionId和消息在Kafka中的offset 接收两种类型的数据AvroPut字节和PutWrapper的Kryo序列化字节
	 * 
	 * @param input
	 */
	public void execute(Tuple input) {

		if (this.dependencyStreamIDSet.contains(input.getSourceStreamId())) {

			try {

				if (hbaseAvailable == false && this.hbaseErrorCount >= 3) {
					long interval = System.currentTimeMillis() - this.hbaseLastFailedTime;
					if (interval >= this.hbaseStatusCheckInterval) {
						this.hbaseAvailable = HBaseUtils.isTableAvailable(this.hBaseConf, hTable.getName());
						if (this.hbaseAvailable == false) {
							this.hbaseLastFailedTime = System.currentTimeMillis();
						}
					}

					if (this.hbaseAvailable == false) {
						if (this.isHBaseFailedHandlerON == true) {
							this.collector.emit(input, input.getValues());
							// this.collector.ack(input);
							return;
						} else {
							// do nothing
						}
					} else {
						// Thread.sleep(100);
					}
				}
				boolean insertSucceed = true;
				boolean flushSucceed = true;
				Put put = null;
				// input Tuple的第一个数据为我们所需要的数据
				Object obj = input.getValue(0);
				// 判断接收的数据类型
				if (obj instanceof byte[]) {
					byte[] bytes = input.getBinary(0);
					put = this.serializationUtil.toPut(this.serializationUtil.toObject(bytes, this.tupleSchemaClazz));
				} else if (obj instanceof KryoPut) {
					put = ((KryoPut) obj).toPut();
				} else if (obj instanceof AvroPut) {
					// 不存在！！
					put = serializationUtil.toPut(obj);
				} else {
					logger.error("The input is not bytes of KryoPut / AvroPut ,is not KryoPut Object yet");
					return;
				}

				tupleList.add(input);
				try {
					hTable.put(put);
					insertSucceed = true;

				} catch (IOException e) {
					// logger.warn("HBase has something wrong !");
					insertSucceed = false;
				}

				// flush
				if (insertSucceed && tupleList.size() >= this.manualFlushTupleInterval) {
					try {
						this.hbaseAvailable = HBaseUtils.isTableAvailable(this.hBaseConf, hTable.getName());

						if (this.hbaseAvailable == true) {
							hbaseErrorCount = 0;
							hTable.flushCommits();
							// if exception , can not reach
							tupleList.clear();
						} else {

							flushSucceed = false;

						}

					} catch (Exception e) {
						flushSucceed = false;
						this.hbaseAvailable = false;
					}

				}

				if ((this.isHBaseFailedHandlerON == true) && (flushSucceed == false || insertSucceed == false)) {

					// hbase error数增加
					hbaseErrorCount++;
					this.hbaseLastFailedTime = System.currentTimeMillis();
					// 将put直接传给下一个bolt
					for (Tuple t : tupleList) {
						this.collector.emit(input, t.getValues());
					}
					tupleList.clear();

					hTable.clearWriteBuffer();

				}

			} catch (Exception e) {
				logger.warn("Process Tuple failed! ");
			} finally {
				collector.ack(input);
			}

		}

	}

	public void cleanup() {
		logger.info("Clean up environment of the HBase Bolt!");

		try {
			if (hTable != null) {
				hTable.flushCommits();
				hTable.close();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> fieldList = new ArrayList<String>();
		if(this.isHBaseFailedHandlerON == true){
			// 有后续的异常处理
			ArrayList<Emit> emits = this.boltConf.getEmits();
			if(emits!=null && emits.size() == 1 ){
				//只能有一个emit
				Emit emit = emits.get(0);
				String streamId = emit.getStreamID();
				logger.info("streamId--"+streamId);
				List<String> fields = emit.getEmitFields();
				if( streamId != null ){
					declarer.declareStream(streamId, new Fields(fields));
				}else{
					declarer.declare(new Fields(fields));
				}	
				
			}else{
				// 可能是使用者没有写emit标签,
				logger.warn("emit参数没有指定，默认指定var1为field名");
				fieldList.add("var1");
				declarer.declare(new Fields(fieldList));
			}
		}

	}

}
