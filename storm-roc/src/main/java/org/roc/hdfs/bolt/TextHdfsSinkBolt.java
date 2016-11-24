package org.roc.hdfs.bolt;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.roc.configuration.BoltConfiguration;
import org.roc.configuration.TopologyConfiguration;
import org.roc.configuration.BoltConfiguration.BoltConfConstant;
import org.roc.configuration.Relation.Dependency;
import org.roc.hdfs.bolt.rules.ArchiveAction;
import org.roc.hdfs.bolt.rules.CountSyncPolicy;
import org.roc.hdfs.bolt.rules.FileNameFormat;
import org.roc.hdfs.bolt.rules.FileRotationPolicy;
import org.roc.hdfs.bolt.rules.FileSizeRotationPolicy;
import org.roc.hdfs.bolt.rules.SyncPolicy;
import org.roc.hdfs.bolt.rules.TimedRotationPolicy;

import com.google.gson.Gson;

/**
 * 将日志记录以avro对象的形式存入hdfs，可以有两种rotation策略
 * 
 * @author liucheng
 * 
 */

public class TextHdfsSinkBolt implements IRichBolt {
	private static final long serialVersionUID = 1481616368931326966L;
	public static Logger logger = LoggerFactory.getLogger(TextHdfsSinkBolt.class);
	private BoltConfiguration boltConf;
	private String confJSON;
	// 从参数文件得到
	private String hadoopConfPath = null;
	private String hdfsOutputPath = null;
	private String userName = null;
	private String fileNamePrefix = null;
	private int syncInterval = 0;
	private long rotationFileSize = 0L;
	private long rotationTimeInterval = 0L;
	private String textDelimer = null;
	private long tickFreSec = 300L;

	HashSet<String> dependencyStreamIDSet = new HashSet<String>();

	// 不可配置变量
	private OutputCollector collector = null;

	// private long offset = 0L;
	private Configuration conf = null;
	private transient FSDataOutputStream outputStream = null;// 输出流

	// private BufferedWriter out = null;

	private Path currentFile;
	private transient FileSystem fileSystem = null;

	private ArchiveAction rotationAction = null;
	private ArrayList<FileRotationPolicy> rotations = null;
	private SyncPolicy syncPolicy;

	private FileNameFormat fileNameFormat;
	private transient Object writeLock;

	public TextHdfsSinkBolt() {
	}

	public TextHdfsSinkBolt(BoltConfiguration boltConf) {
		this.boltConf = boltConf;
		this.setByConfig();
	}

	public TextHdfsSinkBolt(String json) {
		this.confJSON = json;
		this.setByJSON();
	}

	private void setByConfig() {
		Properties props = this.boltConf.getProperties();
		this.hadoopConfPath = props.getProperty(BoltConfConstant.HADOOP_CONF_PATH);
		this.hdfsOutputPath = props.getProperty(BoltConfConstant.HDFS_OUTPUT_PATH);
		this.userName = props.getProperty(BoltConfConstant.HDFS_USERNAME);
		this.fileNamePrefix = props.getProperty(BoltConfConstant.HDFS_OUTPUT_FILENAME_PREFIX);
		this.syncInterval = Integer.parseInt(props.getProperty(BoltConfConstant.HDFS_SYNC_INTERVAL));
		this.rotationFileSize = Long.parseLong(props.getProperty(BoltConfConstant.HDFS_ROTATION_FILESIZE));
		this.rotationTimeInterval = Long.parseLong(props.getProperty(BoltConfConstant.HDFS_ROTATION_TIME_INTERVAL));
		this.textDelimer = props.getProperty(BoltConfConstant.HDFS_TEXT_DELIMER);
		// 对于配置文件中的\t \001 \n 做特殊处理
		if ((TopologyConfiguration.ASCII_CONTROL_MAP.get(this.textDelimer)) != null) {
			this.textDelimer = TopologyConfiguration.ASCII_CONTROL_MAP.get(this.textDelimer);
		}

		logger.info(this.textDelimer);
		this.tickFreSec = Long.parseLong(props.getProperty(BoltConfConstant.HDFS_TICKFRESEC));

		for (Dependency d : this.boltConf.getDependencies()) {
			this.dependencyStreamIDSet.add(d.getStreamID());
		}

	}

	private void setByJSON() {
		Gson gson = new Gson();
		Map<String, String> map = gson.fromJson(confJSON, Map.class);
		// Todo
	}

	private void setFileSystem(Map stormConf) {

		logger.info("Load  hdfs-site.xml core-site.xml");
		logger.info("hadoopConfPath:" + this.hadoopConfPath);

		Path hdfs_conf = new Path(this.hadoopConfPath + File.separator + "hdfs-site.xml");
		Path core_conf = new Path(this.hadoopConfPath + File.separator + "core-site.xml");
		conf = new Configuration(false);
		// 覆盖配置
		conf.addResource(core_conf);
		conf.addResource(hdfs_conf);

		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		logger.info("[TEST] Get key-value from the hbase-site.xml ( hbase.rootdir=" + conf.get("hbase.rootdir") + "  )");
		try {

			this.fileSystem = FileSystem.get(URI.create(this.hdfsOutputPath), conf, this.userName);

		} catch (IOException e) {
			logger.error("Can't get FileSystem instance ! ", e);
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void prepare(Map stormConf, TopologyContext topolgyContext, OutputCollector collector) {
		logger.info("Preparing HDFS Bolt...");

		logger.info("SUGON -- syncInterval = " + this.syncInterval);

		this.collector = collector;

		logger.info("Get the configurations from Storm Config!");

		// hadoopConfPath = (String) stormConf.get("hadoop.conf.path");
		// 设置FileName , 配置FileSystem
		this.setFileSystem(stormConf);

		// 文件名的格式
		this.fileNameFormat = new FileNameFormat().withPath(this.hdfsOutputPath).withPrefix(this.fileNamePrefix).withExtension(".txt.tmp"); // 以确定不会更改
		fileNameFormat.prepare(stormConf, topolgyContext);

		// set sync policy
		this.syncPolicy = new CountSyncPolicy(this.syncInterval);

		// Rotation 策略1
		this.rotations = new ArrayList<FileRotationPolicy>();
		if (this.rotationFileSize > 0) {
			FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(this.rotationFileSize);
			this.rotations.add(rotationPolicy);
		}
		// Rotation 策略2
		if (this.rotationTimeInterval > 0) {
			TimedRotationPolicy rotationPolicy = new TimedRotationPolicy(this.rotationTimeInterval);
			this.rotations.add(rotationPolicy);
		}
		if (this.rotations.size() <= 0) {
			logger.error("Rotation Policy must be setted !");
			throw new IllegalStateException("Rotation Policy must be specified.");
		}

		// rotation Action
		this.rotationAction = new ArchiveAction();

		try {
			// 得到输出文件路径，构建输出流
			// 不追加~，如果追加会有不完成记录
			this.currentFile = this.createOutputFileForPrepare();
		} catch (Exception e) {
			logger.warn("Error preparing HdfsBolt", e);
			throw new RuntimeException("Error preparing HdfsBolt: " + e.getMessage(), e);
		}

		this.writeLock = new Object();

		logger.info("Prepare end");
	}

	/**
	 * 必须使用修改后的kafka-spout，使得messageId的toString方法包括了paritionId和消息在Kafka中的offset
	 * 
	 * @param input
	 */
	public void execute(Tuple tuple) {
		logger.debug("Processing one tuple in HDFS Bolt ...");

		if (isTickTuple(tuple)) {
			logger.debug("tick ~~");
			try {
				syncAndRotate();
			} catch (Exception e) {
				logger.warn("sync error");
			}

		} else {

			// logger.info("------------------"+dependencyStreamIDSet.size());
			if (this.dependencyStreamIDSet.contains(tuple.getSourceStreamId())) {

				// int partitionId = tuple.getInteger(1);
				// long offset = tuple.getLong(2);
				// 目前只支持string， 如果多类型需要在参数中设置
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

				try {

					if (record != null) {

						synchronized (this.writeLock) {

							record = record + this.textDelimer;
							// logger.info(record);
							//outputStream.writeUTF(record);
							outputStream.writeChars(record);
							outputStream.writeBytes(record);
							// 数据是否需要 sync
							if (this.syncPolicy.mark(tuple)) {

								syncAndRotate();
							}

						}

					}

				} catch (Exception e) {

					logger.error("Write tuple to hdfs failed!  ", e);

					// this.collector.fail(tuple);
				} finally {
					this.collector.ack(tuple);
				}

			}

		}

	}

	private void syncAndRotate() throws Exception {

		// outputStream.hsync();
		this.outputStream.hflush();
		// this.outputStream.flush();
		this.syncPolicy.reset();

		for (FileRotationPolicy rotation : this.rotations) {
			// 是否需要rotation
			if (rotation.mark(this.fileSystem, this.currentFile, this.fileNameFormat)) {
				rotateOutputFile();
				break;
			}
		}
	}

	public void cleanup() {
		logger.info("Clean up environment of the HDFS Bolt!");
		try {
			closeOutputFile();
			this.fileSystem.close();
		} catch (IOException e) {
			logger.warn("Can't close output file stream ");
		}

	}

	protected static boolean isTickTuple(Tuple tuple) {
		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}

	public Map<String, Object> getComponentConfiguration() {

		Config conf = new Config();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, this.tickFreSec);
		return conf;

	}

	public void declareOutputFields(OutputFieldsDeclarer ofd) {

	}

	// / ---------- HDFS 逻辑 ------

	/**
	 * rotate a file
	 */

	protected void rotateOutputFile() throws Exception {
		logger.info("Rotating output file...");
		long start = System.currentTimeMillis();
		synchronized (this.writeLock) {
			closeOutputFile();

			Path newFile = createOutputFile();
			logger.info("Performing file rotation actions.");

			this.rotationAction.execute(this.fileSystem, this.currentFile);

			this.currentFile = newFile;
		}
		long time = System.currentTimeMillis() - start;
		logger.info("File rotation took {} ms.", time);
	}

	private void closeOutputFile() throws IOException {

		try {
			if (this.outputStream != null) {
				this.outputStream.flush();
				IOUtils.cleanup(null, this.outputStream);
			}
		} catch (IOException e) {
			logger.error("avro file close,error!\n" + e.getMessage());
		}
	}

	public FSDataOutputStream createOrGetFSOutputHandler(FileSystem hdfs, Path path, int retry) throws Exception {

		FSDataOutputStream fsOutput = null;
		int errorNum = 0;

		while (errorNum < retry) {
			try {
				if (!hdfs.exists(path)) {
					logger.info(path + " is not exits,create!");
					fsOutput = hdfs.create(path);
				} else {
					logger.info(path + " is  exits, append!");
					fsOutput = hdfs.append(path);
				}
				logger.info(path + " create or append sucess!");
				errorNum = retry;
			} catch (Exception e) {

				logger.error(path + " create Or append error,retry:" + errorNum + " times!\n Error Info:" + e.getMessage());
				errorNum++;
				if (errorNum == retry - 1) {
					this.fileSystem = FileSystem.get(URI.create(this.hdfsOutputPath), conf, "etluser");
				}
				if (errorNum >= retry) {
					throw new Exception(e);
				}
				try {
					Thread.sleep(2000);
					logger.info("Sleep 2 seconds,retry create file!");
				} catch (InterruptedException e1) {
					logger.error("Sleep 2 seconds ,error!\n Error Info:" + e.getMessage());
				}
			}

		}

		return fsOutput;
	}

	/**
	 * execute 中达到rotation要求时调用,创建新文件，新的HDFS输出流
	 */
	private Path createOutputFile() throws Exception {
		logger.info("Create new HDFS and it's FSDataOutputStream ");

		this.outputStream = null;
		Path path = new Path(this.fileNameFormat.getPath(), this.fileNameFormat.getName(System.currentTimeMillis()));

		this.outputStream = this.createOrGetFSOutputHandler(this.fileSystem, path, 5);

		return path;
	}

	/**
	 * prepare中调用，bolt会间断的重启，每次重启新建一个输出流指向一个新的文件，如果有.tmp文件就把他重命名
	 * 
	 * @return
	 * @throws Exception
	 */
	private Path createOutputFileForPrepare() throws Exception {

		logger.info("Create new HDFS and it's FSDataOutputStream In prepare method");
		this.outputStream = null;

		// 处理上次bolt故障文件，并返回一个新的输出文件
		String fileName = this.fileNameFormat.getFileName(this.fileSystem, this.rotationAction);
		Path path = new Path(this.fileNameFormat.getPath(), fileName);

		this.outputStream = this.createOrGetFSOutputHandler(this.fileSystem, path, 5);

		return path;
	}

	/**
	 * prepare中调用，bolt会间断的重启，每次重启新建一个输出流指向一个新的文件，如果有.tmp文件就把他重命名
	 * 
	 * @return
	 * @throws Exception
	 */
	private Path createOutputFileForPrepareAppend() throws Exception {
		logger.info("Create new HDFS and it's FSDataOutputStream In prepare method");
		this.outputStream = null;

		// 处理上次bolt故障文件，并返回一个
		String fileName = this.fileNameFormat.getFileName(this.fileSystem, this.rotations);

		Path path = new Path(this.fileNameFormat.getPath(), fileName);

		int retry = 5;
		int errorNum = 0;

		while (errorNum < retry) {
			try {
				if (!this.fileSystem.exists(path)) {
					logger.info(path + " is not exits,create!");
					this.outputStream = this.fileSystem.create(path);
				} else {
					logger.info(path + " is  exits, append!");
					this.outputStream = this.fileSystem.append(path);

				}

				logger.info(path + " create or append sucess!");
				errorNum = retry;
			} catch (Exception e) {

				logger.error(path + " create Or append error,retry:" + errorNum + " times!\n Error Info:" + e.getMessage());
				errorNum++;
				if (errorNum == retry - 1) {
					this.fileSystem = FileSystem.get(URI.create(this.hdfsOutputPath), conf, this.userName);
				}
				if (errorNum >= retry) {
					throw new Exception(e);
				}
				try {
					Thread.sleep(2000);
					logger.info("Sleep 2 seconds,retry create file!");
				} catch (InterruptedException e1) {
					logger.error("Sleep 2 seconds ,error!\n Error Info:" + e.getMessage());
				}
			}

		}

		return path;
	}

}
