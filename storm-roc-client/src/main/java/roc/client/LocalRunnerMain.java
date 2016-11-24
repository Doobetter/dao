package roc.client;

import org.roc.topology.TopologySubmiter;
/**
 * 用于本地测试
 * 需要注意的是配置文件中hadoop-conf的设置路径必须设置为local路径
 * @author LC
 *
 */
public class LocalRunnerMain {

	public static void main(String[] args) {
		String arg1 = "--commonFilePath=D:/workspace2/roc/roc-common/conf/topology-common.xml";
		String arg2 = "--specialFilePath=D:/workspace2/roc-client/src/main/java/example/hdfs02/topology-example0201.xml";
		String arg3 = "--noteFileDir=D:/";
		String arg4 = "--runType=local";
		String arg5 = "--stormLimitFilePath=D:/workspace2/roc/roc-common/conf/storm-limit.xml";
		String[] Args = new String[] { arg1,arg2,arg3,arg4,arg5};
		TopologySubmiter.main(Args);
	}

}
