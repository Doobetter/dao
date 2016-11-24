package org.roc.configuration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.roc.configuration.Relation.Dependency;
import org.roc.configuration.Relation.Emit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
/**
 * 解析配置文件后将bolt的信息封装成一个BoltConfiguration对象
 * 主要成员：
 * 1. id ,为bolt的componentID
 * 2. type, 是bolt的类型, 可以是 枚举{@link BoltType}BoltType中的typeAttribute变量中的任何一个字符串 
 * 3. dependencies, 是bolt的输入数据模式集合，参见{@link Dependency}
 * 4. emits, 是bolt的输出数据模式集合，参见{@link Emit}
 * 5. processClass, bolt的处理类，只有type是{@link BoltType}.CUSTOMER 时才有用
 * 6. properties, 非结构化配置参数，在xml文件中<properties>中的内容
 * @author LC
 *
 */
public class BoltConfiguration implements Serializable {
	private static final long serialVersionUID = 2885663925123530890L;
	public static Logger logger = LoggerFactory.getLogger(BoltConfiguration.class);
	private String id = "";
	// pretty json
	static Gson gson = new GsonBuilder().setPrettyPrinting().create();
	private BoltType type = BoltType.HBASE;

	private ArrayList<Dependency> dependencies = new ArrayList<Dependency>();

	private ArrayList<Emit> emits = new ArrayList<Emit>();

	private Properties properties = null;

	private String processClass = null;

	public static final String DEFAULT_SCHEMA_CLASS = "NO";
	public static final String DEFAULT_STREAM_ID = "NO";

	/**
	 * 添加一个输出关系
	 * @param streamID
	 * @param schema
	 */
	public void addEmit(String streamID, ArrayList<FieldSchema> schema) {
		this.emits.add(new Emit(streamID, schema));
	}

	/**
	 * 添加一个输入关系
	 * @param componentID 
	 * @param streamID
	 * @param streamGrouping
	 * @param partitionFields 如果是streamGrouping是fields时，partitionFields即为各个fields的字符串
	 * @param schema
	 */
	public void addDependency(String componentID, String streamID, String streamGrouping, String partitionFields, ArrayList<FieldSchema> schema) {
		this.dependencies.add(new Dependency(componentID, streamID, schema));
	}

	public void setType(String type) {
		// 查找！
		BoltType[] types = BoltType.class.getEnumConstants();
		for (BoltType t : types) {
			if (type.equals(t.getTypeAttribute())) {
				this.type = t;
				break;
			}
		}
	}

	/**
	 * 得到properties中的配置
	 * @param key
	 * @return
	 */
	public Object getConf(String key) {
		return this.properties.get(key);
	}

	/**
	 * 得到当前Bolt中所有KryoClass
	 * @return
	 */
	public Set<String> getKryoClass() {
		Set<String> set = new HashSet<String>();
		ArrayList<Dependency> dependencies = this.getDependencies();
		// 添加kryoput相关的类
		CollectionUtils.addAll(set, FieldSchemaType.getKryoPutRelatedClasses());

		for (Dependency d : dependencies) {
			ArrayList<FieldSchema> fields = d.getFields();

			for (FieldSchema field : fields) {

				if (field.getType() != null) {
					if (field.getType().equals(FieldSchemaType.KRYO) || field.getType().equals(FieldSchemaType.KRYOPUT)) {
						set.add(field.getClazzName());
					}
				}

			}

		}
		ArrayList<Emit> emits = this.getEmits();
		for (Emit e : emits) {

			ArrayList<FieldSchema> fields = e.getFields();
			for (FieldSchema field : fields) {

				if (field.getType() != null) {
					if (field.getType().equals(FieldSchemaType.KRYO) || field.getType().equals(FieldSchemaType.KRYOPUT)) {
						set.add(field.getClazzName());
					}
				}

			}
		}

		return set;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public BoltType getType() {
		return type;
	}

	public void setType(BoltType type) {
		this.type = type;
	}

	public ArrayList<Dependency> getDependencies() {
		return dependencies;
	}

	public void setDependencies(ArrayList<Dependency> dependencies) {
		this.dependencies = dependencies;
	}

	public ArrayList<Emit> getEmits() {
		return emits;
	}

	public void setEmits(ArrayList<Emit> emits) {
		this.emits = emits;
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	public String getProcessClass() {
		return processClass;
	}

	public void setProcessClass(String processClass) {
		this.processClass = processClass;
	}

	public String toJSON() {
		return gson.toJson(this, this.getClass());
	}

	public static BoltConfiguration getFromJSON(String json) {
		return gson.fromJson(json, BoltConfiguration.class);
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("id=").append(this.id).append("\n");
		sb.append("type=").append(this.type).append("\n");

		return sb.toString();
	}

	public static void main(String[] args) {
		System.out.println(BoltType.HBASE == BoltType.HBASE);

	}

	/**
	 * Bolt 所需的配置字符串常量，可以在以后维护中添加，然后在各个实际Bolt中使用
	 * @author LC
	 *
	 */
	public static class BoltConfConstant {

		public static final String BOLT_EXECUTORNUMBER = "bolt.executorNumber";
		public static final String HADOOP_CONF_PATH = "hadoop.conf.path";
		// for hbase bolt
		public static final String HBASE_REGIONSERVERNUMBER = "hbase.regionServerNumber";
		public static final String HBASE_AUTOFLUSH = "hbase.autoFlush";
		public static final String HBASE_WRITEBUFFERSIZE = "hbase.writeBufferSize";
		public static final String HBASE_TABLENAME = "hbase.tableName";
		public static final String HBASE_MANUALFLUSHLIMIT = "hbase.manualFlushLimit";
		public static final String HBASE_STATUS_CHECK_INTERVAL = "hbase.statusCheckInterval";
		public static final String HBASE_IS_HBASE_FAILED_HANDLER_ON = "hbase.isHBaseFailedHandlerON";
		// for hdfs bolt
		public static final String HDFS_OUTPUT_PATH = "hdfs.output.path";
		public static final String HDFS_USERNAME = "hdfs.userName";
		public static final String HDFS_OUTPUT_FILENAME_PREFIX = "hdfs.output.filename.prefix";
		public static final String HDFS_SYNC_INTERVAL = "hdfs.sync.interval.tuple";
		public static final String HDFS_ROTATION_FILESIZE = "hdfs.rotation.fileSize";
		public static final String HDFS_ROTATION_TIME_INTERVAL = "hdfs.rotation.time.interval";
		public static final String HDFS_TEXT_DELIMER = "hdfs.text.delimer";
		public static final String HDFS_AVRO_SCHEMA = "hdfs.avro.schema";
		public static final String HDFS_TICKFRESEC = "hdfs.tickFreSec";
		// for redis
		public static final String REDIS_HOSTANDPORT = "redis.hostAndPort";
		public static final String REDIS_COMMAND_TIMEOUT = "redis.commandTimeout";
		public static final String REDIS_MAX_REDIRECTIONS = "redis.maxRedirections";
	}
}
