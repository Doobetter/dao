package org.roc.topology;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.roc.configuration.BoltConfiguration;
import org.roc.configuration.BoltType;
import org.roc.configuration.SpoutConfiguration;
import org.roc.configuration.SpoutType;
import org.roc.configuration.TopologyConfiguration;
import org.roc.configuration.BoltConfiguration.BoltConfConstant;
import org.roc.configuration.Relation.Dependency;
import org.roc.configuration.SpoutConfiguration.SpoutConfConstant;
import org.roc.exception.ConfigurationException;
/**
 *  用于构建Storm的拓扑作业
 * @author LC
 *
 */
public class TopologyFactory {
	public static Logger logger = LoggerFactory.getLogger(TopologyFactory.class);

	/**
	 * 通过配置对象生成IRichBolt
	 * 
	 * @param sptConf
	 * @return
	 */
	public IRichSpout getSpoutByConf(SpoutConfiguration sptConf) {
		IRichSpout spout = null;
		try {
			Class<?> clazz = Class.forName(sptConf.getType().getSpoutClass());
			Constructor<?> constructor = clazz.getConstructor(SpoutConfiguration.class);
			Object obj = constructor.newInstance(sptConf);
			if (sptConf.getType() == SpoutType.KAFKA) {
				// kafka 特殊
				Method mothod = clazz.getMethod("build", null);
				spout = (IRichSpout) mothod.invoke(obj, null);
			} else {
				spout = (IRichSpout) obj;
			}

		} catch (Exception e) {
			logger.warn("Spout 参数反射出错 ,id=" + sptConf.getId());
			e.printStackTrace();
		}
		return spout;

	}

	/**
	 * 通过配置文件，生成Bolt，默认Bolt都是IRichBolt，即需要程序员处理ack/fail
	 * 
	 * @param bltConf
	 * @return
	 */
	public IRichBolt getBoltByConf(BoltConfiguration bltConf) {
		IRichBolt bolt = null;
		Class<?> clazz = null;
		try {
			if (bltConf.getType() == BoltType.CUSTOMER) {
				// 客户自定义的class，可以使直接实现IRichBolt，也可以继承CustomerETLBolt
				clazz = Class.forName(bltConf.getProcessClass());
			} else {
				clazz = Class.forName(bltConf.getType().getBoltClass());
			}
			logger.info("" + bltConf.getProcessClass());
			Constructor<?> constructor = clazz.getConstructor(BoltConfiguration.class);
			bolt = (IRichBolt) constructor.newInstance(bltConf);

		} catch (Exception e) {
			logger.warn("bolt 参数反射出错，id=" + bltConf.getId());
			e.printStackTrace();
		}
		return bolt;
	}

	/**
	 * 通过tplConf构建一个StormTopology
	 * 
	 * @param tplConf
	 * @return
	 * @throws ConfigurationException 
	 */
	public StormTopology buildStormTopology(TopologyConfiguration tplConf) throws ConfigurationException {
		TopologyBuilder builder = new TopologyBuilder();

		List<SpoutConfiguration> sptConfs = tplConf.getSpoutList();
		List<BoltConfiguration> bltConfs = tplConf.getBoltList();

		for (SpoutConfiguration sptConf : sptConfs) {
			IRichSpout spout = this.getSpoutByConf(sptConf);
			Number parallelism_hint = Double.parseDouble(sptConf.getProperties().getProperty(SpoutConfConstant.SPOUT_EXECUTORNUMBER));
			builder.setSpout(sptConf.getId(), spout, parallelism_hint);
		}

		for (BoltConfiguration bltConf : bltConfs) {
			IRichBolt bolt = this.getBoltByConf(bltConf);
			Number parallelism_hint = Double.parseDouble(bltConf.getProperties().getProperty(BoltConfConstant.BOLT_EXECUTORNUMBER));
			BoltDeclarer boltDeclarer = builder.setBolt(bltConf.getId(), bolt, parallelism_hint);
			ArrayList<Dependency> list = bltConf.getDependencies();
			for (Dependency dependency : list) {
				if ("shuffle".equals(dependency.getStreamGrouping())) {
					// 数据随机分发到下一个Bolt
					if (dependency.getStreamID() != null) {
						boltDeclarer.shuffleGrouping(dependency.getComponentID(), dependency.getStreamID());
					} else {
						boltDeclarer.shuffleGrouping(dependency.getComponentID());
					}
				} else if ("fields".equals(dependency.getStreamGrouping())) {
					// 数据按field发送到下一个bolt的相应实例中
					String partitionFields = dependency.getPartitionFields();
					if (partitionFields != null && !"".equals(partitionFields)) {
						// fields 在配置参数 <partitionFields> 中指定
						Fields fields = new Fields(partitionFields.split(","));
						boltDeclarer.fieldsGrouping(dependency.getComponentID(), dependency.getStreamID(), fields);

					} else {
						//TODO 
						logger.warn("You not set partitionFields, we default use 'var' for  field grouping !");
						
						boltDeclarer.fieldsGrouping(dependency.getComponentID(), dependency.getStreamID(), new Fields("var"));
					}

				}
				else if ( "all".equals(dependency.getStreamGrouping())){
					// 每个元组发送到下一个bolt所有实例中
					if (dependency.getStreamID() != null) {
						boltDeclarer.allGrouping(dependency.getComponentID(), dependency.getStreamID());
					} else {
						boltDeclarer.allGrouping(dependency.getComponentID());
					}
					
				}else{
					throw new ConfigurationException("Only field/shuffle/all be supported! Check your xml streamGrouping tag");
				}

			}

		}
		StormTopology topology = builder.createTopology();
		return topology;
	}

	/**
	 * 注册所有的Kryo序列化的类
	 * 
	 * @param tplConf
	 * @param config
	 */
	public void registerKryoClasses(TopologyConfiguration tplConf, Config config) {
		//不使用Java自带的序列化机制
		config.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, false);
		
		Set<String> set = new HashSet<String>();
		List<BoltConfiguration> bltConfs = tplConf.getBoltList();
		for (BoltConfiguration bltConf : bltConfs) {
			set.addAll(bltConf.getKryoClass());
		}
		for (String kryoClass : set) {
			try {
				config.registerSerialization(Class.forName(kryoClass));
			} catch (ClassNotFoundException e) {
				logger.error("检查使用Kryo序列化的类路径是否正确", e);
			}
		}
	}

}
