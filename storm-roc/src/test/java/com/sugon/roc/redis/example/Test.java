package com.sugon.roc.redis.example;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

public class Test {

	public static void test(){
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
		JedisCluster jc = new JedisCluster(jedisClusterNodes,poolConfig);
		//jc.select(0);
		//jc.set("foo", "bar");
		String value = jc.get("b");

		System.out.println(value);
	}
	public static void testData(){
		String [] s = {"liucheng","Yao","Micheal"};
		String className = s.getClass().getSimpleName();
		System.out.println(className);
		
		String s1 = "";
		String className1 = s1.getClass().getSimpleName();
		System.out.println(className1);
		
		HashMap<String,String> map = new HashMap<String, String>();
		className = ((Map<String,String>) map).getClass().getSimpleName();
		System.out.println(className);
	}
	public static void main(String[] args) {
		test();
	}

}
