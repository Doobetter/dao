package org.roc.topology;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.roc.configuration.ConfigurationMain;
import org.roc.configuration.TopologyConfiguration;
import org.roc.exception.ConfigurationException;
import org.roc.topology.cmd.Cmd;

/**
 * 作业提交
 * 
 * @author LC
 *
 */
public class TopologySubmiter {
	public static Logger logger = LoggerFactory.getLogger(TopologySubmiter.class);

	public static void main(String[] args) {

		Cmd cmd = new Cmd("Topology Submit Parameter");
		cmd.addParam("commonFilePath", "公用参数文件topology-common.xml绝对路径");
		cmd.addParam("specialFilePath", "当前Topology特有参数文件绝对路径");
		cmd.addParam("noteFileDir", "最终参数保存位置(结尾没有文件位置分隔符)");
		cmd.addParam("stormLimitFilePath", "上线资源检查配置文件位置,if ");
		cmd.addParam("runType", "local 或者  remote");
		cmd.parse(args);

		String commonFilePath = cmd.getArgValue("commonFilePath");
		String specialFilePath = cmd.getArgValue("specialFilePath");
		String noteFileDir = cmd.getArgValue("noteFileDir");
		String runType = cmd.getArgValue("runType");
		String stormLimitFilePath = cmd.getArgValue("stormLimitFilePath");
		// 解析配置文件，生成配置文件对象
		logger.info("Parse configuration files ... ");
		ConfigurationMain confMain = new ConfigurationMain(commonFilePath, specialFilePath,stormLimitFilePath);
		TopologyConfiguration tplConf = confMain.getTopologyProps();
		logger.info("Parse configuration files end! ");
		// 构建Storm拓扑作业
		logger.info("Generate a Storm toplogy job ... ");
		TopologyFactory tplGenerator = new TopologyFactory();
		StormTopology topology = null;
		try {
			topology = tplGenerator.buildStormTopology(tplConf);
		} catch (ConfigurationException e1) {
			logger.error("Configuration XML", e1);
		}
		logger.info("Generate a Storm toplogy job end! ");
		// 注册Kryo序列化的类
		Config config = tplConf.getStormConfig();
		tplGenerator.registerKryoClasses(tplConf, config);
		// 提交作业
		logger.info("Submitter storm job ...");
		try {
			if("local".equals(runType)){
				runTopologyLocally(tplConf.getTopologyID(), config, topology, 120);
			}else if("remote".equals(runType)){
				runTopologyRemotely(tplConf.getTopologyID(), config, topology);
			}
			//StormSubmitter.submitTopology(tplConf.getTopologyID(), config, topology);
			logger.info("Submitter storm job end!");
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		} catch (InterruptedException e){
			e.printStackTrace();
		} catch (AuthorizationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// 记录配置信息
		Date now = new Date();
		SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
		String fileName = noteFileDir + File.separator + tplConf.getTopologyID() + ft.format(now) + ".txt";
		File recordFile = new File(fileName);
		FileOutputStream out = null;
		try {
			out = new FileOutputStream(recordFile);
			out.write(Bytes.toBytes(cmd.getPrintCmd()));
			out.write(Bytes.toBytes("以下是Topology的配置参数：\n"));
			confMain.saveConfiguration(tplConf, out);
			out.flush();

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
				}
			}
		}
		logger.info("作业的提交信息已经保存到" + fileName);
	}

	/**
	 * 本地运行
	 * @param topologyName
	 * @param conf
	 * @param topology
	 * @param runtimeInSeconds
	 * @throws InterruptedException
	 */
	public static void runTopologyLocally( String topologyName, Config conf, StormTopology topology, int runtimeInSeconds) throws InterruptedException {
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(topologyName, conf, topology);
		Thread.sleep((long) runtimeInSeconds * MILLIS_IN_SEC);
		cluster.killTopology(topologyName);
		cluster.shutdown();
	}

	/**
	 * storm 集群上运行
	 * @param topologyName
	 * @param conf
	 * @param topology
	 * @throws AlreadyAliveException
	 * @throws InvalidTopologyException
	 * @throws AuthorizationException 
	 */
	public static void runTopologyRemotely( String topologyName, Config conf,StormTopology topology) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		StormSubmitter.submitTopology(topologyName, conf, topology);
	}

	private static final int MILLIS_IN_SEC = 1000;
}
