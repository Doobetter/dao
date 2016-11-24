package org.roc.utils;

import org.apache.hadoop.hbase.client.Put;

/**
 * 序列化工具接口
 * @author LC
 *
 */
public interface ISerializationUtil {
	public <T> byte [] toBytes(T t);
	public <T> T toObject(byte [] bs, Class<T> clazz);
	
	public <T> Put toPut(T t);
}
