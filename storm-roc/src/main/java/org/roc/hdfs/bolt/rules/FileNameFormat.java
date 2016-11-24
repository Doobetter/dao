/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.roc.hdfs.bolt.rules;

import org.apache.storm.task.TopologyContext;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;


/**
 * Creates file names with the following format:
 * 
 * <pre>
 *     {prefix}{componentId}-{taskId}-{rotationNum}-{timestamp}{extension}
 * </pre>
 * 
 * For example:
 * 
 * <pre>
 *     MyBolt-5-7-1390579837830.txt
 * </pre>
 * 
 * By default, prefix is empty and extenstion is ".txt".
 * 
 */
public class FileNameFormat{
	private String componentId;
	private int taskId;
	private String path = "/storm";
	private String prefix = "";
	private String extension = ".file";


	/**
	 * Overrides the default prefix.
	 * 
	 * @param prefix
	 * @return
	 */
	public FileNameFormat withPrefix(String prefix) {
		this.prefix = prefix;
		return this;
	}

	/**
	 * Overrides the default file extension.
	 * 
	 * @param extension
	 * @return
	 */
	public FileNameFormat withExtension(String extension) {
		this.extension = extension;
		return this;
	}

	public FileNameFormat withPath(String path) {
		this.path = path;
		return this;
	}


	public void prepare(Map conf, TopologyContext topologyContext) {
		this.componentId = topologyContext.getThisComponentId();
		this.taskId = topologyContext.getThisTaskId();

	}

	public static String timeStampToDateFormat(long timeStamp, String format) {

		SimpleDateFormat df = new SimpleDateFormat(format);// 定义格式

		String str = df.format(timeStamp);

		return str;
	}


	public String getName(long timeStamp) {
		//format: prefix-yyyymmddhhMM-componentId-taskId-timeStamp.extension
		String dateStr = timeStampToDateFormat(timeStamp, "yyyyMMddHHmm");
		return this.prefix + "-" + dateStr + "-" + this.componentId + "-"
				+ this.taskId + "-" + timeStamp + this.extension;
	}

	class RegexFileNameFilter implements PathFilter {
		private final String regex;

		public RegexFileNameFilter(String regex) {
			this.regex = regex;
		}

		@Override
		public boolean accept(Path path) {
			return path.getName().toString().matches(regex);
		}
	}
	/**
	 * 如果上次hdfs文件写入中断，对其执行归档，返回一个新的文件路径（适用于记录有格式要求的文件）
	 * @param fs
	 * @param action
	 * @return
	 */
	public String getFileName(FileSystem fs,ArchiveAction action) {
		long timeStamp = System.currentTimeMillis();
		String rs = this.getName( timeStamp);
		// 依赖于文件名的命名规则,找到当前componentId，taskID丢弃的文件
		String regex = ".*-" + this.componentId + "-" + this.taskId + ".*tmp";
		FileStatus[] list = null;
		try {
			list = fs.listStatus(new Path(this.path), new RegexFileNameFilter(
					regex));

			if (list != null && list.length > 0) {
				for (FileStatus f : list) {	
					action.execute(fs, f.getPath());
				}
			}

		} catch (IOException e) {

			e.printStackTrace();
			return rs;
		}

		return rs;

	}

	/**
	 * 可以在文件没有达到rotation要求时，在文件尾继续追加,在bolt的prepare中使用
	 * 
	 * @param fs
	 * @param rotations
	 * @param rotationActions
	 * @return
	 */
	public String getFileName(FileSystem fs,
			ArrayList<FileRotationPolicy> rotations) {
		long timeStamp = System.currentTimeMillis();
		String rs = this.getName(timeStamp);
		// 依赖于文件名的命名规则,找到当前componentId，taskID丢弃的文件
		String regex = ".*-" + this.componentId + "-" + this.taskId + ".*tmp";
		FileStatus[] list = null;
		try {
			list = fs.listStatus(new Path(this.path), new RegexFileNameFilter(
					regex));

			if (list != null && list.length > 0) {
				for (FileStatus f : list) {
					for (FileRotationPolicy p : rotations) {
						if (!p.mark(fs, f.getPath(),this)) {
							rs = f.getPath().getName();
							return rs;
						}
					}

				}
			}

		} catch (IOException e) {
			e.printStackTrace();
			return rs;
		}
		return rs;
	}

	public String getPath() {
		return this.path;
	}

	public String getTimeStamp(String fileName){
		return fileName.substring(fileName.lastIndexOf("-")+1,fileName.indexOf(this.extension));
	}
	public static void main(String[] args) {
		String dateStr = timeStampToDateFormat(System.currentTimeMillis(),
				"yyyyMMddHHmm");
		System.out.println(dateStr);

		String str = "dsfss_-sdr.ser-201412101526-data_to_hdfs_bolt_id-2-1418106604810.json.tmp";

		System.out.println(str.matches(".*\\d{12}-data_to_hdfs_bolt_id-2.*"));

		System.out.println(str.substring(str.lastIndexOf("-") + 1,
				str.indexOf(".json")));
		
	}

}
