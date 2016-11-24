package org.roc.configuration;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;



import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ConfigurationUtils {
	private static final Log LOG = LogFactory.getLog(ConfigurationUtils.class);

	public  static  HashMap<String,Object> loadProperties(String str) throws IOException{
		
		HashMap<String, Object> result = new HashMap<String, Object>();
		
		Properties props = new Properties();

		ByteArrayInputStream st = new ByteArrayInputStream(str.getBytes("UTF-8"));
		// 加载公共参数
		props.load(st);

		Set<Object> keySet = props.keySet();
		String key = "";
		String val = "";
		for (Object k : keySet) {
			key = (String) k;

			val = ((String) props.get(k)).trim();

			result.put(key.trim(), getRealTypeObject(val));

		}
		
		return result;
	}
	
	public static HashMap<String,Object> propsToMap(Properties props){
		
		HashMap<String, Object> result = new HashMap<String, Object>();
		
	
		Set<Object> keySet = props.keySet();
		String key = "";
		String val = "";
		for (Object k : keySet) {
			key = (String) k;
			val = ((String) props.get(k)).trim();

			result.put(key.trim(), val);

		}
		
		return result;
	}
	public static Object getRealTypeObject(String str) {
		if (str.matches("^(false|true|False|True|ture)$")) {
			//System.out.println("boolean");
			return Boolean.parseBoolean(str);
		} else if (str.matches("^[0-9]+$")) {
			//System.out.println("int");
			return Long.parseLong(str);
		} else if (str.matches("^[0-9]+( *\\* *[0-9]+)*$")) {
			// 3 * 1024 * 1024 这种乘法串的值算出来
			String trimed = str.replace(" ", "");
			if (!str.contains("*"))
				return Integer.parseInt(trimed);
			String[] numbers = trimed.split("\\*");
			long product = 1;
			for (String num : numbers) {
				product *= Long.parseLong(num);
			}
			return product;
		} else if (str.matches("^[0-9]+\\.[0-9]+$")) {
			//System.out.println("double");
			return Double.parseDouble(str);

		} else {
			//System.out.println("string");
			return str;
		}

	}
	public static Class<?> getType(String str){
		if (str.matches("^(false|true|False|True|ture)$")) {
			return Boolean.class;
		} else if (str.matches("^[0-9]+$")) {
			return Long.class;
		} else if (str.matches("^[0-9]+( *\\* *[0-9]+)*$")) {
			return Long.class;
		} else if (str.matches("^[0-9]+\\.[0-9]+$")) {	
			return Double.class;
		} else {
			return String.class;
		}
	}
	
	public static boolean isNumberType(String str){
		if(str.matches("^[0-9]+(\\.[0-9]+)*")){
			return true;
		}else{
			return false;
		}
	}
	
	/**
	 * 
	 * @param dir
	 *            could be conf | data .etc that dir in audit-tracker
	 * @return
	 */
	public static String getAbsolutePath(String dir) {
		URL url = ConfigurationUtils.class.getProtectionDomain().getCodeSource()
				.getLocation();
		String filePath = null;
		try {
			filePath = URLDecoder.decode(url.getPath(), "utf-8");// 转化为utf-8编码
			System.out.println(filePath);
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (filePath.endsWith(".jar")) {// 可执行jar包运行的结果里包含".jar"
			// 截取路径中的jar包名
			filePath = filePath.substring(0,
					filePath.lastIndexOf(File.separator) + 1);
		}
		// jar 包所在文件夹
		// 或者 classes 或者 test-class
		File file = new File(filePath);

		// String projectPath = file.getAbsoluteFile().getParentFile()
		String projectPath = file.getAbsoluteFile().getParentFile()
				.getParentFile().getAbsolutePath();
		String abPath = projectPath + File.separator + dir;
		System.out.println("[INFO]" + abPath);
		return abPath;
	}
	
	public static void main(String[] args) {
	     String str = "123";
	     String str1 = "123.231";
	     System.out.println(isNumberType(str));
	     System.out.println(isNumberType(str1));
	}

}
