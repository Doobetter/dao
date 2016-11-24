package org.roc.hbase.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HBaseUtils {

	public static HBaseAdmin admin = null;

	public static boolean isTableAvailable(Configuration conf,
			TableName tableName) {
		boolean rs = false;
		try {
			if (admin == null) {
				admin = new HBaseAdmin(conf);
			}
			// 表是否存在
			rs = admin.tableExists(tableName);
			// 表是否可用
			rs = rs && admin.isTableEnabled(tableName);
			//HBaseAdmin.checkHBaseAvailable(conf);
			return rs;
		} catch (IOException e) {
			return rs;
		}

	}
}
