package com.sugon.roc.redis.example;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.roc.redis.KryoRedisTuple;
import org.roc.redis.KryoRedisTuple.ValueType;
import org.roc.redis.KryoRedisTuple.WriteMethod;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

public class RedisUtils {
	public static JedisCluster jc = null;
	public static void insert(KryoRedisTuple data ){
		String key = data.getKey();
		Object value = data.getValue();
		ValueType type = data.getType();
		WriteMethod method = data.getMethod() ;
		int expireSeconds = data.getExpireSeconds();
		
		
		if(ValueType.STRING == type){
			//字符串
			jc.set( key, (String)value );
			
		}
		else if(ValueType.LIST == type){
			if(WriteMethod.LPUSH == method){
				// list left 
				jc.lpush( key, (String[])value );
			}else if(WriteMethod.RPUSH == method){
				// list right
				jc.rpush( key, (String[])value );
			}
		}
		else if(ValueType.HASH == type){
			//散列表（HashMap）
			jc.hmset( key, (Map<String,String>)value );
		}
		else if(ValueType.SET == type){
			// set
			jc.sadd(key, (String[])value );
		}
		else if(ValueType.SORTEDSET == type){
			// sorted set
			jc.zadd(key, (Map<String,Double>)value);
		}
		if(expireSeconds > 0){
			jc.expire(key, (int) expireSeconds);
		}
		
	}
	
	public static void execute(String method, Class<?> valueType , String key ,Object value){
		try {
			Object rs = null;
			Method m = JedisCluster.class.getMethod(method, valueType);
			if(value!=null){
				rs = m.invoke(jc, key, value);
			}
			else{
				rs = m.invoke(jc, key);
			}
			
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void initJC(){
		Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
		//Jedis Cluster will attempt to discover cluster nodes automatically
		jedisClusterNodes.add(new HostAndPort("10.0.31.188", 17000));
		jedisClusterNodes.add(new HostAndPort("10.0.31.189", 17000));
		jedisClusterNodes.add(new HostAndPort("10.0.31.190", 17000));
		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
		poolConfig.setMaxTotal(500);
		poolConfig.setMaxIdle(5);
		poolConfig.setMaxWaitMillis(1000*60);
		poolConfig.setTestOnBorrow(true);
		jc = new JedisCluster(jedisClusterNodes,poolConfig);
	}
	
	
	public static void main(String[] args){
		
		initJC();
		KryoRedisTuple tuple= null;
	/*	// string
		tuple = new RedisTupleKryo("pv","123",ValueType.STRING);
		insert(tuple);
		System.out.println(jc.get("pv"));*/
		
		
		// list
		String [] s = {"liucheng","Yao","Michel"};
		s.getClass().getName();
//		/tuple = new RedisTupleKryo("students",s,ValueType.LIST,WriteMethod.LPUSH);
		//insert(tuple);
		//System.out.println(jc.llen("students"));
		//
		
		jc.close();

	}
}
