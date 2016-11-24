package org.roc.configuration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;

import org.apache.storm.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Topology 作业的配置信息都会封装到TopologyConfiguration对象
 * @author LC
 *
 */
public class TopologyConfiguration implements Serializable {

	private static final long serialVersionUID = 8041178742650434617L;

	private static final Logger logger = LoggerFactory.getLogger(TopologyConfiguration.class);

	// topology Name is id
	private String topologyID;
	// pretty json
	static Gson gson = new GsonBuilder().setPrettyPrinting().create();
	// public HashMap<String,Object> topologyConfig = new HashMap<String,Object>();
	private Properties properties = null;

	private ArrayList<SpoutConfiguration> spoutList = new ArrayList<SpoutConfiguration>();
	private ArrayList<BoltConfiguration> boltList = new ArrayList<BoltConfiguration>();

	private Config stormConfig = null;

	/**
	 * 对于配置文件中的\t \001 \n 做特殊处理 ,例如 在xml写的是\001，但是代码中得到的是字符串\\001，我们需要的是\001
	 */
	public static final HashMap<String, String> ASCII_CONTROL_MAP = new HashMap<String, String>();
	static {
		ASCII_CONTROL_MAP.put("\\001", "\001");
		ASCII_CONTROL_MAP.put("\\002", "\002");
		ASCII_CONTROL_MAP.put("\\003", "\003");
		ASCII_CONTROL_MAP.put("\\t", "\t");
		ASCII_CONTROL_MAP.put("\\n", "\n");
		ASCII_CONTROL_MAP.put("\\r", "\r");
		ASCII_CONTROL_MAP.put("\\r\\n", "\r\n");
	}

	// call after parse xml doc
	public Config getStormConfig() {
		if (stormConfig == null) {
			stormConfig = new Config();
			if (properties == null) {
				logger.warn("topologyConfig is null, please init firstly");
				return null;
			}
			Set<?> keySet = properties.keySet();
			String key = "";
			String val = "";
			for (Object k : keySet) {
				key = (String) k;
				val = ((String) properties.get(k)).trim();
				stormConfig.put(key.trim(), ConfigurationUtils.getRealTypeObject(val));
			}
		}
		return stormConfig;
	}

	
	public void addSpout(SpoutConfiguration spout) {
		this.spoutList.add(spout);
	}

	public void addBolt(BoltConfiguration bolt) {
		this.boltList.add(bolt);
	}

	public String getTopologyID() {
		return topologyID;
	}

	public void setTopologyID(String topologyID) {
		this.topologyID = topologyID;
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	public ArrayList<SpoutConfiguration> getSpoutList() {
		return spoutList;
	}

	public void setSpoutList(ArrayList<SpoutConfiguration> spoutList) {
		this.spoutList = spoutList;
	}

	public ArrayList<BoltConfiguration> getBoltList() {
		return boltList;
	}

	public void setBoltList(ArrayList<BoltConfiguration> boltList) {
		this.boltList = boltList;
	}

	public void setStormConfig(Config stormConfig) {
		this.stormConfig = stormConfig;
	}

	public String toJSON() {

		return gson.toJson(this, this.getClass());
	}

	public static TopologyConfiguration getFromJSON(String json) {

		return gson.fromJson(json, TopologyConfiguration.class);
	}


}
