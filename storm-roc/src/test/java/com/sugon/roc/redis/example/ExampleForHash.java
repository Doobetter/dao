package com.sugon.roc.redis.example;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import com.google.gson.Gson;

public class ExampleForHash {

	public static JedisCluster jc = null;
	public static Gson gson = new Gson();
	public static Long insertPostBean( PostBean post ){
		//首先获得新文章的ID
		Long postID = jc.incr("post:count");
		//jc.incr(key)
		/*try {
			Method m = JedisCluster.class.getMethod("incr", String.class);
			m.invoke(jc, "");
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
		}*/
		System.out.println(postID);
		// 把文章以HashMap形式存入Hash型键值中
		jc.hmset("post:"+postID+":data", post.toMap());
		
		

		return postID;
		
	}
	
	public static void getPostBean(long postID){
		//从Redis中读取文章数据
		
		String title = jc.hget("post:"+postID+":data", "title");
		
		System.out.println(title);
	
		Long viewCount = jc.incr("post:"+postID+":view");
		System.out.println("view : "+ viewCount);
	}
	
	
	
	public static void initJC(){
		
		
		Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
		//Jedis Cluster will attempt to discover cluster nodes automatically
		jedisClusterNodes.add(new HostAndPort("10.0.31.188", 17000));
		jedisClusterNodes.add(new HostAndPort("10.0.31.189", 17000));
		jedisClusterNodes.add(new HostAndPort("10.0.31.190", 17000));	
		jc = new JedisCluster(jedisClusterNodes);
	}
	
	public static void main(String[] args) {
		initJC();
		//PostBean post = new PostBean("雅虎北研中心宣布将关闭 员工获“N+4”补偿","新浪科技讯 3月18日消息，雅虎高级副总裁今日在内部正式宣布将关闭雅虎北京全球研发中心（以下简称“雅虎北研”）。一位内部员工告诉新浪科技，雅虎将提供给员工“N+4”补偿，“具体的赔偿还需要和各自的管理层谈”。"," 新浪科技","2015年03月18日10:55");
		//Long id = insertPostBean(post);
		//getPostBean(id);
		//String a = "34";
		//String [] array = {"1","2"};
		//Object obj  = a;
		
		//jc.rpush("list1", (String[]) obj);
		
		System.out.println(jc.llen("豫EUU616"));
		//System.out.println(jc.scard("kkwz"));
		
		
	/*	
		try {
			Method m = JedisCluster.class.getMethod("incr", String.class);
			Long postID = (Long) m.invoke(jc, "post:count",null);
			System.out.println(postID);
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
		}*/
		
	}
}
