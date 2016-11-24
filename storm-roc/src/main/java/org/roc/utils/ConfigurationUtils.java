package org.roc.utils;

import java.io.File;
import java.net.URL;
import java.net.URLDecoder;

public class ConfigurationUtils {
	/**
	 *  适用于  
	 *  |--project
	 *    |--bin
	 *    |--lib
	 *       |--XXX.jar
	 *    |--conf
	 *    |--webapp  
	 *  获取conf路径，可以getAbsolutePath("conf")
	 * @param dir
	 * @return
	 */
	public static String getAbsolutePath(String dir) {
		
		URL url = ConfigurationUtils.class.getProtectionDomain().getCodeSource().getLocation();
		String filePath = null;
		try {
			filePath = URLDecoder.decode(url.getPath(), "UTF-8");
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		String projectPath = null;
		if (filePath.endsWith(".jar")) {
			// 可执行jar包运行的结果里包含".jar"
			// 截取路径中的jar包名
			projectPath = new File(filePath).getAbsoluteFile().getParentFile().getParentFile().getAbsolutePath();
		}
		else {
			// jar 包所在文件夹 for TEST
			// 或者 classes 或者 test-class
			projectPath = new File("").getAbsolutePath();
		}
		String abPath = projectPath + File.separator + dir;
		
		return abPath;
	}

}
