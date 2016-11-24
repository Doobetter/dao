package org.roc.redis;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
/**
 * 作为 {@link com.sugon.roc.redis.RedisClusterSinkBolt}的输入数据
 * 1.key 是redis中的key
 * 2.value是redis中所支持的String、list、set、hash、sortedset类型的数据
 * 3.type 是在RedisClusterSinkBolt写入数据时用的，详见{@link ValueType}
 * 4.method是写入数据使用的方法 ，详见{@link WriteMethod}
 * 5. expireSeconds 是数据过期时间单位为秒，可让数据过期自动删除
 * @author LC
 *
 */
public class KryoRedisTuple implements Serializable{
	private static final long serialVersionUID = 4554049504760250858L;
	private String key = null;
	private Object value = null;
	private ValueType type = null;
	private WriteMethod method = null;
	int expireSeconds = -1;
	
	public KryoRedisTuple(){
		
	}
	
	public KryoRedisTuple(String key, Object value,ValueType type){
		this.key = key;
		this.value = value;
		this.type = type;
		this.method = null;
	}
	public KryoRedisTuple(String key, Object value, ValueType type,  WriteMethod method ){
		this.key = key;
		this.value = value;
		this.type = type;
		this.method = method;
	}
	
	public KryoRedisTuple(String key, Object value, ValueType type,  WriteMethod method, int  expireSeconds){
		this.key = key;
		this.value = value;
		this.type = type;
		this.method = method;
		this.expireSeconds = expireSeconds;
	}
	
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public Object getValue() {
		return value;
	}
	public void setValue(Object value) {
		this.value = value;
	}
	public ValueType getType() {
		return type;
	}
	public void setType(ValueType type) {
		this.type = type;
	}
	public WriteMethod getMethod() {
		return method;
	}
	public void setMethod(WriteMethod method) {
		this.method = method;
	}
	public int getExpireSeconds() {
		return expireSeconds;
	}
	public void setExpireSeconds(int expireSeconds) {
		this.expireSeconds = expireSeconds;
	}

	public void reset(){
		this.key = null;
		this.value = null;
		this.type = null;
		this.method = null;
		this.expireSeconds = -1;
	}
	
	public boolean valueCheck(){
		String valueClass = this.value.getClass().getSimpleName();
		if(valueClass.contains("Map")){
			return true;
		}
		List<String> list = Arrays.asList("String[]","String","HashMap","Map");
		return list.contains(valueClass);

	}
	
	/**
	 * redis中支持的数据类型
	 * @author LC
	 *
	 */
	public enum ValueType{
		/**
		 * redis中支持的字符串类型
		 */
		STRING,
		/**
		 * redis中支持的列表类型
		 */
		LIST,
		/**
		 * redis中支持的散列类型
		 */
		HASH,
		/**
		 * redis中支持的集合类型
		 */
		SET,
		/**
		 * redis中支持的有序集合类型
		 */
		SORTEDSET
		
	}
	/**
	 * 支持部分redis写入函数
	 * @author LC
	 *
	 */
	public enum WriteMethod{
		/**
		 * 写入String类型的数据
		 */
		SET,
		/**
		 * 向列表类型数据左侧添加数据，JedisCluster中lpush函数
		 */
		LPUSH,
		/**
		 * 向列表类型数据右侧添加数据，JedisCluster中rpush函数
		 */
		RPUSH,
		/**
		 * 向散列类型数据添加数据，JedisCluster中hmset函数
		 */
		HMSET,
		/**
		 * 向集合类型数据添加数据，JedisCluster中sadd函数
		 */
		SADD,
		ZADD,
		INCR,
		INCR_BY;
		
		
	}
}
