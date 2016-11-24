package org.roc.configuration;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.utils.Utils;
import org.roc.configuration.Relation.Dependency;
import org.roc.configuration.Relation.Emit;
import org.roc.exception.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
/**
 * ConfigurationMain的主要功能
 * 1.解析共有配置文件
 * 2.解析特有配置文件覆盖共有配置，生成{@link TopologyConfiguration}对象
 * 3.将{@link TopologyConfiguration}对象格式化地保存于一个记录文件中
 * @author LC
 *
 */
public class ConfigurationMain {
	public static Logger logger = LoggerFactory.getLogger(ConfigurationMain.class);
	// private Properties commonProperties;
	/**
	 * topology-common.xml中的配置常量
	 */
	public final List<String> COMPONENT_TYPES = Arrays.asList("topology", "kafka-spout", "text-hdfs-spout", "avro-hdfs-spout", "customer-bolt", "hbase-bolt",
			"redis-bolt", "text-hdfs-bolt", "avro-hdfs-bolt");
	private Map<String, Properties> commonProperties = null;
	private Map<String, Properties> limitProperties = null;
	private TopologyConfiguration tplConf = null;
	private String  limitPropertiesFilePath = "";
	private String commonPropertiesFilePath = "";
	private String specialPropertiesFilePath = "";

	public ConfigurationMain() {
	}

	public ConfigurationMain(String commonFile, String specialFile) {
		this.commonPropertiesFilePath = commonFile;
		this.specialPropertiesFilePath = specialFile;

	}
	public ConfigurationMain(String commonFile, String specialFile ,String limitFile) {
		
		this.commonPropertiesFilePath = commonFile;
		this.specialPropertiesFilePath = specialFile;
		this.limitPropertiesFilePath = limitFile;

	}
	
	public Map<String, Properties> getStormLimitProps() {
		if (limitProperties == null) {
			limitProperties = new HashMap<String, Properties>();
			loadStormLimitResources(this.limitPropertiesFilePath);
		}

		return limitProperties;
	}
	protected void loadStormLimitResources(String filePath) {
		Document doc = null;
		Element root = null;

		DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
		// ignore all comments inside the xml file
		docBuilderFactory.setIgnoringComments(true);

		try {
			DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
			doc = builder.parse(filePath); 
			root = doc.getDocumentElement();

			if (!"topology".equals(root.getTagName())) {
				logger.error("bad conf file storm-limit.xml : top-level element not <topology> ");
				throw new ConfigurationException("bad conf file: top-level element not <topology> ");
			}

			// System.out.println(root.getElementsByTagName("properties").getLength());
			NodeList propertiesChilds = root.getElementsByTagName("properties");
			int propsLen = propertiesChilds.getLength();
			for (int i = 0; i < propsLen; i++) {
				Node propNode = propertiesChilds.item(i);
				if (!(propNode instanceof Element))
					continue;
				Element elem = (Element) propNode;
				if ("properties".equals(elem.getTagName())) {
					String parentNodeName = elem.getParentNode().getNodeName();

					if ("topology".equals(parentNodeName)) {
						this.limitProperties.put("topology", parseProperties(elem));
					}
					else if("spout".equals(parentNodeName)){
						this.limitProperties.put("spout", parseProperties(elem));
					}
					else if("bolt".equals(parentNodeName)){
						this.limitProperties.put("bolt", parseProperties(elem));
					}
				}

			}

		}
		catch (SAXException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		catch (ParserConfigurationException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 获得共有参数配置
	 * 
	 * @return
	 */
	public Map<String, Properties> getCommonProps() {
		if (commonProperties == null) {
			commonProperties = new HashMap<String, Properties>();
			loadCommonResources(this.commonPropertiesFilePath);
		}

		return commonProperties;
	}

	/**
	 *  解析topology-common.xml
	 * @param filePath
	 */
	protected void loadCommonResources(String filePath) {
		Document doc = null;
		Element root = null;

		DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
		// ignore all comments inside the xml file
		docBuilderFactory.setIgnoringComments(true);

		try {
			DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
			doc = builder.parse(filePath); 
			root = doc.getDocumentElement();

			if (!"topology".equals(root.getTagName())) {
				logger.error("bad conf file topology-common.xml : top-level element not <topology> ");
				throw new ConfigurationException("bad conf file: top-level element not <topology> ");
			}

			// System.out.println(root.getElementsByTagName("properties").getLength());
			NodeList propertiesChilds = root.getElementsByTagName("properties");
			int propsLen = propertiesChilds.getLength();
			for (int i = 0; i < propsLen; i++) {
				Node propNode = propertiesChilds.item(i);
				if (!(propNode instanceof Element))
					continue;
				Element elem = (Element) propNode;
				if ("properties".equals(elem.getTagName())) {
					String parentNodeName = elem.getParentNode().getNodeName();
					// System.out.println(parentNodeName);

					mark: if ("topology".equals(parentNodeName)) {
						this.commonProperties.put("topology", parseProperties(elem));
					}
					else {
						SpoutType[] spoutTypes = SpoutType.class.getEnumConstants();
						for (SpoutType t : spoutTypes) {
							if (parentNodeName.equals(t.getTypeAttribute())) {
								this.commonProperties.put(t.getTypeAttribute(), parseProperties(elem));
								break mark;
							}
						}

						BoltType[] boltTypes = BoltType.class.getEnumConstants();
						for (BoltType t : boltTypes) {
							if (parentNodeName.equals(t.getTypeAttribute())) {
								this.commonProperties.put(t.getTypeAttribute(), parseProperties(elem));
								break mark;
							}
						}

					}
				}

			}

		}
		catch (SAXException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		catch (ParserConfigurationException e) {
			e.printStackTrace();
		}

	}

	/**
	 * 解析 <properties>标签，公用函数
	 * 
	 * @param propElem
	 * @return
	 */
	private Properties parseProperties(Element propElem) {
		Properties properties = new Properties();

		if (!"properties".equals(propElem.getTagName()))
			logger.error("bad properties conf Element: top-level element not <properties>");
		NodeList props = propElem.getChildNodes();

		for (int i = 0; i < props.getLength(); i++) {
			Node propNode = props.item(i);
			if (!(propNode instanceof Element))
				continue;
			Element prop = (Element) propNode;

			if (!"property".equals(prop.getTagName()))
				logger.error("bad properties conf Element: element not <property>");
			NodeList fields = prop.getChildNodes();
			String attr = null;
			String value = null;

			for (int j = 0; j < fields.getLength(); j++) {
				Node fieldNode = fields.item(j);
				if (!(fieldNode instanceof Element))
					continue;
				Element field = (Element) fieldNode;
				if ("name".equals(field.getTagName()) && field.hasChildNodes())
					attr = field.getFirstChild().getNodeValue().trim();
				if ("value".equals(field.getTagName()) && field.hasChildNodes()) {
					value = field.getFirstChild().getNodeValue().trim();
					if (value.matches("^[0-9]+( *\\* *[0-9]+)*$")) {
						// 3 * 1024 * 1024 这种乘法串的值算出来
						String trimed = value.replace(" ", "");
						if (value.contains("*")) {
							String[] numbers = trimed.split("\\*");
							long product = 1;
							for (String num : numbers) {
								product *= Long.parseLong(num);
							}
							value = "" + product;
						}
					}
				}
			}
			// System.out.println(attr + "," + value);
			properties.put(attr, value);
		}

		return properties;
	}

	/**
	 * 获得每个Topology特有的参数配置，覆盖共有参数配置
	 * 
	 * @return
	 */
	public TopologyConfiguration getTopologyProps() {
		// common must be parsed firstly
		if (commonProperties == null) {
			getCommonProps();
		}

		if (tplConf == null) {
			tplConf = new TopologyConfiguration();
			loadSpecialResources(this.specialPropertiesFilePath);
		}
		
		
		// 只有使用的是三个参数的构造函数才会去设置limit
		if(!"".equals(this.limitPropertiesFilePath)){
			// 添加limit
			if(this.limitProperties == null){
				getStormLimitProps();
			}
			
			overRideProperties(this.tplConf.getProperties(),this.limitProperties.get("topology"));
			
			for(SpoutConfiguration sptConf : this.tplConf.getSpoutList()){
				overRideProperties(sptConf.getProperties(),this.limitProperties.get("spout"));
			}
			
			for(BoltConfiguration bltConf : this.tplConf.getBoltList()){
				overRideProperties(bltConf.getProperties(),this.limitProperties.get("bolt"));
			}
			
		}
		
		
		
		
		
		return tplConf;
	}

	
	public void overRideProperties(Properties props, Properties limit){

		Set<Object> keys = limit.keySet();
		for(Object key : keys){
			if(props.containsKey(key)){
				if(ConfigurationUtils.isNumberType(limit.getProperty((String)key))){
					double var1 = Double.parseDouble(props.getProperty((String)key));
					double var2 =  Double.parseDouble(limit.getProperty((String)key));
					if(var2<var1){
						//取小的
						props.put(key, limit.getProperty((String)key));
					}
					
				}else{
					props.put(key, limit.get(key));
				}
			}
		}
	}
	
	protected void loadSpecialResources(String absPath) {

		DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
		// ignore all comments inside the xml file
		docBuilderFactory.setIgnoringComments(true);
		try {
			DocumentBuilder dbBuilder = docBuilderFactory.newDocumentBuilder();
			Document doc = dbBuilder.parse(absPath);

			// topology
			Element root = doc.getDocumentElement();
			if (!"topology".equals(root.getTagName())) {
				logger.error("bad conf file: top-level element not <topology> ");
			}

			tplConf.setTopologyID(root.getAttribute("id").trim());
			tplConf.setProperties(this.commonProperties.get("topology"));
			// topology's properties，解析topology的properties
			NodeList tplChilds = root.getChildNodes();
			int tplChildsLen = tplChilds.getLength();
			for (int i = 0; i < tplChildsLen; i++) {
				Node childNode = tplChilds.item(i);
				if (!(childNode instanceof Element))
					continue;
				Element elem = (Element) childNode;
				if ("properties".equals(elem.getTagName())) {
					tplConf.getProperties().putAll(parseProperties(elem));
					break;
				}
			}
			// tplConf.getTopologyConfig().list(System.out);

			// 解析spout
			NodeList list = root.getElementsByTagName("spout");
			int len = list.getLength();
			for (int i = 0; i < len; i++) {
				Node childNode = list.item(i);
				if (!(childNode instanceof Element))
					continue;
				Element elem = (Element) list.item(i);
				tplConf.addSpout(parseSpout(elem, tplConf.getTopologyID()));

			}

			// 解析bolt
			list = root.getElementsByTagName("bolt");
			len = list.getLength();
			for (int i = 0; i < len; i++) {
				Node childNode = list.item(i);
				if (!(childNode instanceof Element))
					continue;
				Element elem = (Element) list.item(i);
				tplConf.addBolt(parseBolt(elem));
			}

		}
		catch (ParserConfigurationException e) {
			e.printStackTrace();
		}
		catch (SAXException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * 解析<spout>标签
	 * 
	 * @param spoutElem
	 * @return
	 */
	private SpoutConfiguration parseSpout(Element spoutElem, String toplogyID) {
		SpoutConfiguration conf = new SpoutConfiguration(toplogyID);
		conf.setId(spoutElem.getAttribute("id").trim());
		String type = spoutElem.getAttribute("type").trim();
		if (!this.COMPONENT_TYPES.contains(type)) {
			logger.error("Spout type is setted wrong!! ", new Exception("Spout Type Error"));
		}
		else {
			// 加载共有参数
			conf.setProperties((Properties) this.commonProperties.get(type).clone());
		}
		conf.setType(type);

		Node propNode = spoutElem.getElementsByTagName("properties").item(0);
		if (propNode instanceof Element) {
			Element elem = (Element) propNode;
			conf.getProperties().putAll((this.parseProperties(elem)));

		}
		// conf.getProperties().list(System.out);
		// System.out.println(conf.toString());
		return conf;

	}

	/**
	 * 解析<bolt>标签
	 * 
	 * @param boltElem
	 * @return
	 */
	private BoltConfiguration parseBolt(Element boltElem) {
		BoltConfiguration conf = new BoltConfiguration();
		conf.setId(boltElem.getAttribute("id").trim());
		String type = boltElem.getAttribute("type").trim();

		if (!this.COMPONENT_TYPES.contains(type)) {
			throw new ConfigurationException("Bolt type is setted wrong!! your config is " +type);
		}
		else {
			// 加载共有参数
			conf.setProperties((Properties) this.commonProperties.get(type).clone());
		}
		conf.setType(type);

		Node propNode = boltElem.getElementsByTagName("properties").item(0);
		if (propNode instanceof Element) {
			Element elem = (Element) propNode;
			// 加载特有的参数，覆盖共有参数
			conf.getProperties().putAll((this.parseProperties(elem)));
		}

		if (conf.getType() == BoltType.CUSTOMER) {
			conf.setProcessClass(boltElem.getElementsByTagName("processClass").item(0).getFirstChild().getNodeValue());
		}
		conf.setType(type);

		//解析emits标签
		NodeList emits = boltElem.getElementsByTagName("emit");
		int lenx = emits.getLength();
		for (int i = 0; i < lenx; i++) {
			if (!(emits.item(i) instanceof Element)) {
				continue;
			}
			Element emit = (Element) emits.item(i);

			NodeList emitChilds = emit.getChildNodes();
			int leny = emitChilds.getLength();
			String streamID = Utils.DEFAULT_STREAM_ID;
			ArrayList<FieldSchema> fields = new ArrayList<FieldSchema>();

			for (int j = 0; j < leny; j++) {
				if (!(emitChilds.item(j) instanceof Element)) {
					continue;
				}

				Element emitChild = (Element) emitChilds.item(j);
				String nodeName = emitChild.getTagName();
				if ("streamID".equals(nodeName) && emitChild.hasChildNodes()) {
					streamID = emitChild.getFirstChild().getNodeValue().trim();
				}
				else if ("tupleSchema".equals(nodeName) && emitChild.hasChildNodes()) {
					//fields = new ArrayList<FieldSchema>();
					
					Element tupleSchema = emitChild;
					NodeList schemaChilds = tupleSchema.getChildNodes();
					int lenz = schemaChilds.getLength();
					for (int z = 0; z < lenz; z++) {
						if (!(schemaChilds.item(z) instanceof Element)) {
							continue;
						}
						
						Element schemaChild = (Element) schemaChilds.item(z);
						String nodeNamez = schemaChild.getTagName();
						FieldSchema  field = null;
						if("fieldSchema".equals(nodeNamez)&& schemaChild.hasChildNodes()) {
			
							NodeList fieldChilds = schemaChild.getChildNodes();
							int len_f = fieldChilds.getLength();
							
							String fieldName = null;
							String schemaType = null;
							String schemaClazz = null;
							for (int f = 0; f < len_f; f++) {
								if (!(fieldChilds.item(f) instanceof Element)) {
									continue;
								}
								Element fieldChild = (Element) fieldChilds.item(f);
								String fieldTagName = fieldChild.getTagName();
							
								if ("name".equals(fieldTagName)  && fieldChild.hasChildNodes() ){
									fieldName = fieldChild.getFirstChild().getNodeValue().trim();
								}
								else if ("type".equals(fieldTagName) && fieldChild.hasChildNodes()) {
									schemaType = fieldChild.getFirstChild().getNodeValue().trim();
								}
								else if ("schemaClazz".equals(fieldTagName) && fieldChild.hasChildNodes()) {
									schemaClazz = fieldChild.getFirstChild().getNodeValue().trim();
								}
								
							}
							// TODO 分析合理性
							if (schemaType != null ||fieldName!=null||schemaClazz!=null) {
								// 只需要type不为null，avro-put和kryo-put的schemaClazz可以为null
								field = new FieldSchema(fieldName,schemaType, schemaClazz);
								fields.add(field);
							}
							
						}
							
					}
					
				}
			}
			conf.addEmit(streamID, fields);
		}

		//解析dependencies标签
		NodeList dependencies = boltElem.getElementsByTagName("dependency");
		lenx = dependencies.getLength();
		if (lenx <= 0) {
			throw new ConfigurationException("Bolt[" + conf.getId() + "]  must have a dependency, check the configuration ");
		}
		for (int i = 0; i < lenx; i++) {
			if (!(dependencies.item(i) instanceof Element)) {
				continue;
			}
			Element dependency = (Element) dependencies.item(i);
			NodeList dependencyChilds = dependency.getChildNodes();
			int childLen = dependencyChilds.getLength();
			String componentID = null;
			String streamID = Utils.DEFAULT_STREAM_ID;
			String streamGrouping = "group"; //default;
			String partitionFields = null;
			ArrayList<FieldSchema> fields = new ArrayList<FieldSchema> ();
			for (int j = 0; j < childLen; j++) {
				if (!(dependencyChilds.item(j) instanceof Element)) {
					continue;
				}
				Element dependencyChild = (Element) dependencyChilds.item(j);
				String nodeName = dependencyChild.getTagName();
				if ("componentID".equals(nodeName) && dependencyChild.hasChildNodes()) {
					componentID = dependencyChild.getFirstChild().getNodeValue().trim();
				}
				else if ("streamID".equals(nodeName) && dependencyChild.hasChildNodes()) {
					streamID = dependencyChild.getFirstChild().getNodeValue().trim();
				}
				else if ("streamGrouping".equals(nodeName) && dependencyChild.hasChildNodes()) {
					// 需要检查 stream Grouping 是否合格，可以推迟到生成topology时
					streamGrouping = dependencyChild.getFirstChild().getNodeValue().trim();
				}
				else if ("partitionFields".equals(nodeName) && dependencyChild.hasChildNodes()) {
					partitionFields = dependencyChild.getFirstChild().getNodeValue().trim();
				}
				else if ("tupleSchema".equals(nodeName) && dependencyChild.hasChildNodes()) {
					
					
					Element tupleSchema = dependencyChild;
					NodeList schemaChilds = tupleSchema.getChildNodes();
					int lenz = schemaChilds.getLength();
					for (int z = 0; z < lenz; z++) {
						if (!(schemaChilds.item(z) instanceof Element)) {
							continue;
						}
						
						Element schemaChild = (Element) schemaChilds.item(z);
						String nodeNamez = schemaChild.getTagName();
						FieldSchema  field = null;
						if("fieldSchema".equals(nodeNamez)&& schemaChild.hasChildNodes()) {
			
							NodeList fieldChilds = schemaChild.getChildNodes();
							int len_f = fieldChilds.getLength();
							
							String fieldName = null;
							String schemaType = null;
							String schemaClazz = null;
							for (int f = 0; f < len_f; f ++) {
								if (!(fieldChilds.item(f) instanceof Element)) {
									continue;
								}
								Element fieldChild = (Element) fieldChilds.item(f);
								String fieldTagName = fieldChild.getTagName();
							
								if ("name".equals(fieldTagName)  && fieldChild.hasChildNodes() ){
									fieldName = fieldChild.getFirstChild().getNodeValue().trim();
								}
								else if ("type".equals(fieldTagName) && fieldChild.hasChildNodes()) {
									schemaType = fieldChild.getFirstChild().getNodeValue().trim();
								}
								else if ("schemaClazz".equals(fieldTagName) && fieldChild.hasChildNodes()) {
									schemaClazz = fieldChild.getFirstChild().getNodeValue().trim();
								}
								// TODO 分析合理性
							
							}
							if (schemaType != null  || schemaClazz!=null  || fieldName!=null) {
								// 只需要type不为null，avro-put和kryo-put的schemaClazz可以为null
								field = new FieldSchema(fieldName,schemaType, schemaClazz);
								fields.add(field);
							}
						}
						
							

					}
					

				}
			}
			if (componentID == null) {
				logger.warn("conf in dependency has no componentID !!!");
			}
			conf.addDependency(componentID, streamID,  streamGrouping, partitionFields ,fields);
		}

		return conf;
	}

	/**
	 * 将TopologyConfiguration对象输入到输出流
	 * @param tplConf
	 * @param out
	 */
	public void saveConfiguration(TopologyConfiguration tplConf, OutputStream out) {
		Document doc = null;
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		try {
			DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
			doc = docBuilder.newDocument();

			Element topology = doc.createElement("topology");
			doc.appendChild(topology);

			topology.setAttribute("id", tplConf.getTopologyID());
			// for properties
			Properties props = tplConf.getProperties();
			if (props.size() > 0) {
				Element propertiesElement = doc.createElement("properties");
				topology.appendChild(propertiesElement);
				Set<Object> keySet = props.keySet();
				String key = "";
				String val = "";
				for (Object k : keySet) {
					key = (String) k;
					val = ((String) props.get(k));
					Element prop = doc.createElement("property");
					propertiesElement.appendChild(prop);

					Element name = doc.createElement("name");
					name.appendChild(doc.createTextNode(key));
					prop.appendChild(name);

					Element value = doc.createElement("value");
					value.appendChild(doc.createTextNode(val));
					prop.appendChild(value);

				}
			}
			Element components = doc.createElement("components");
			topology.appendChild(components);
			// for spouts
			ArrayList<SpoutConfiguration> spts = tplConf.getSpoutList();
			for (SpoutConfiguration sptConf : spts) {
				components.appendChild(createSpoutElement(doc, sptConf));
			}
			// for bolts
			ArrayList<BoltConfiguration> blts = tplConf.getBoltList();
			for (BoltConfiguration bltConf : blts) {
				components.appendChild(createBoltElement(doc, bltConf));
			}
			// 输出
			prettyPrint(doc, out);

		}
		catch (ParserConfigurationException e) {
			e.printStackTrace();
		}
		// root elements

	}

	private Element createSpoutElement(Document doc, SpoutConfiguration sptConf) {
		Element spout = doc.createElement("spout");
		// doc.appendChild(spout);
		// for attributes
		spout.setAttribute("id", sptConf.getId());
		spout.setAttribute("type", sptConf.getType().getTypeAttribute());

		// for properties
		Properties props = sptConf.getProperties();
		if (props.size() > 0) {
			Element propertiesElement = doc.createElement("properties");
			spout.appendChild(propertiesElement);
			Set<Object> keySet = props.keySet();
			String key = "";
			String val = "";
			for (Object k : keySet) {
				key = (String) k;
				val = ((String) props.get(k));
				Element prop = doc.createElement("property");
				propertiesElement.appendChild(prop);

				Element name = doc.createElement("name");
				name.appendChild(doc.createTextNode(key));
				prop.appendChild(name);

				Element value = doc.createElement("value");
				value.appendChild(doc.createTextNode(val));
				prop.appendChild(value);

			}
		}
		return spout;

	}

	private Element createBoltElement(Document doc, BoltConfiguration bltConf) {
		Element bolt = doc.createElement("bolt");
		// doc.appendChild(bolt);
		// for attributes
		bolt.setAttribute("id", bltConf.getId());
		bolt.setAttribute("type", bltConf.getType().getTypeAttribute());
		// for dependencies
		ArrayList<Dependency> dependencies = bltConf.getDependencies();
		if (dependencies != null && dependencies.size() > 0) {
			Element dependenciesElement = doc.createElement("dependencies");
			for (Dependency d : dependencies) {
				Element dependency = doc.createElement("dependency");

				if (d.getComponentID() != null) {
					Element componentID = doc.createElement("componentID");
					componentID.appendChild(doc.createTextNode(d.getComponentID()));
					dependency.appendChild(componentID);
				}
				if(d.getStreamGrouping() != null){
					Element streamGrouping = doc.createElement("streamGrouping");
					streamGrouping.appendChild(doc.createTextNode(d.getStreamGrouping()));
					dependency.appendChild(streamGrouping);
				}
				if(d.getPartitionFields() != null){
					Element partitionFields = doc.createElement("partitionFields");
					partitionFields.appendChild(doc.createTextNode(d.getPartitionFields()));
					dependency.appendChild(partitionFields);
				}
				
				if (d.getStreamID() != null) {
					Element streamID = doc.createElement("streamID");
					streamID.appendChild(doc.createTextNode(d.getStreamID()));
					dependency.appendChild(streamID);
				}
				

				ArrayList<FieldSchema> fields= d.getFields();
				if(fields !=  null){
					Element schema = doc.createElement("tupleSchema");
					dependency.appendChild(schema);
					for (FieldSchema field : fields){
						Element fieldE = doc.createElement("fieldSchema");
				
						if(field.getFieldName()!=null){
							Element name = doc.createElement("name");
							name.appendChild(doc.createTextNode(field.getFieldName()));
							fieldE.appendChild(name);
						}
						
						if(field.getType()!=null){
							Element type = doc.createElement("type");
							type.appendChild(doc.createTextNode(field.getType().getTypeName()));
							fieldE.appendChild(type);
						}
						if(field.getClazzName()!=null){
							Element schemaClazz = doc.createElement("schemaClazz");
							schemaClazz.appendChild(doc.createTextNode(field.getClazzName()));
							fieldE.appendChild(schemaClazz);
						}
						schema.appendChild(fieldE);
				
					}
				
				}
				

				dependenciesElement.appendChild(dependency);
			}
			bolt.appendChild(dependenciesElement);
		}
		// for emit
		ArrayList<Emit> emits = bltConf.getEmits();
		if (emits != null && emits.size() > 0) {
			Element emitsElement = doc.createElement("emits");
			bolt.appendChild(emitsElement);

			for (Emit e : emits) {
				Element emit = doc.createElement("emit");
				emitsElement.appendChild(emit);

				if (e.getStreamID() != null) {
					Element streamID = doc.createElement("streamID");
					streamID.appendChild(doc.createTextNode(e.getStreamID()));
					emit.appendChild(streamID);
				}

				ArrayList<FieldSchema> fields= e.getFields();
				if(fields !=  null){
					Element schema = doc.createElement("tupleSchema");
					emit.appendChild(schema);
					for (FieldSchema field : fields){
						Element fieldE = doc.createElement("fieldSchema");
						
						if(field.getFieldName()!=null){
							Element name = doc.createElement("name");
							name.appendChild(doc.createTextNode(field.getFieldName()));
							fieldE.appendChild(name);
						}
						
						if(field.getType()!=null){
							Element type = doc.createElement("type");
							type.appendChild(doc.createTextNode(field.getType().getTypeName()));
							fieldE.appendChild(type);
						}
						if(field.getClazzName()!=null){
							Element schemaClazz = doc.createElement("schemaClazz");
							schemaClazz.appendChild(doc.createTextNode(field.getClazzName()));
							fieldE.appendChild(schemaClazz);
						}
					
						schema.appendChild(fieldE);
					}
				
				}
				/*if (tupeSchema != null) {
					Element schema = doc.createElement("tupleSchema");
					Element type = doc.createElement("type");
					type.appendChild(doc.createTextNode(tupeSchema.getType().getTypeName()));
					Element schemaClazz = doc.createElement("schemaClazz");
					schemaClazz.appendChild(doc.createTextNode(tupeSchema.getClazzName()));
					schema.appendChild(type);
					schema.appendChild(schemaClazz);
					emit.appendChild(schema);

				}*/
			}

		}

		if (bltConf.getType() == BoltType.CUSTOMER) {
			Element clazz = doc.createElement("processClass");
			clazz.appendChild(doc.createTextNode(bltConf.getProcessClass()));
			bolt.appendChild(clazz);
		}

		// for properties
		Properties props = bltConf.getProperties();
		if (props.size() > 0) {
			Element propertiesElement = doc.createElement("properties");
			bolt.appendChild(propertiesElement);
			Set<Object> keySet = props.keySet();
			String key = "";
			String val = "";
			for (Object k : keySet) {
				key = (String) k;
				val = ((String) props.get(k));
				Element prop = doc.createElement("property");
				propertiesElement.appendChild(prop);

				Element name = doc.createElement("name");
				name.appendChild(doc.createTextNode(key));
				prop.appendChild(name);

				Element value = doc.createElement("value");
				value.appendChild(doc.createTextNode(val));
				prop.appendChild(value);

			}
		}
		return bolt;
	}

	/**
	 * 将xml的Document对象数据格式化传入到输出流
	 * 
	 * @param doc
	 * @param out
	 */
	public static final void prettyPrint(Document doc, OutputStream out) {

		try {
			Transformer tf = TransformerFactory.newInstance().newTransformer();
			tf.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
			tf.setOutputProperty(OutputKeys.INDENT, "yes");
			tf.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
			tf.transform(new DOMSource(doc), new StreamResult(out));
		}
		catch (TransformerConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (TransformerFactoryConfigurationError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (TransformerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) throws IOException {

		String commonFile = "D:\\workspace3\\storm-roc\\conf\\topology-common.xml";
		//String specialFile ="D:/workspace2/roc/roc-common/conf/topology-x.xml";

		// String specialFile =
		// "D:/workspace2/roc-client/src/main/java/example/hdfs03/topology-example03.xml";
		String specialFile = "C:/Users/LC/Desktop/shop-roc/shop-roc.xml";
		
		String limitFile = "D:\\workspace3\\storm-roc\\conf\\storm-limit.xml";
		
		
		System.out.println(specialFile);
		
		ConfigurationMain confMain = new ConfigurationMain(commonFile, specialFile, limitFile);
		TopologyConfiguration tplConf = confMain.getTopologyProps();

		//System.out.println("---" + tplConf.getBoltList().get(0).getDependencies().get(0).getFields().get(0).getTupleFiledDelimer() + "---");

		String json = tplConf.toJSON();
		System.out.println(json);

		// save
		FileOutputStream out = new FileOutputStream(new File("d:/book.xml"));
		StringBuffer buf = new StringBuffer();
		buf.append("sdfsfs你好\n");
		out.write(Bytes.toBytes(buf.toString()));
		confMain.saveConfiguration(tplConf, out);

	}

}
