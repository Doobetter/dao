package com.sugon.roc.redis.example;


import java.util.HashSet;
import java.util.Set;

import com.google.gson.Gson;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;


public class ExampleForString {
	public static JedisCluster jc = null;
	public static Gson gson = new Gson();
	public static Long insertPostBean( PostBean post ){
		//首先获得新文章的ID
		Long postID = jc.incr("post:count");
		System.out.println(postID);
		//将博客文章的诸多元素序列化成字符串
		String jsonPost = gson.toJson(post);  
		//把序列化后的字符串存一个入字符串类型的键中
		jc.set( "post:"+postID+":data",  jsonPost);
		return postID;
		
	}
	
	public static void getPostBean(long postID){
		//从Redis中读取文章数据
		String jsonPost = jc.get("post:"+postID+":data");
		System.out.println(jsonPost);
		//将文章数据反序列化成文章的各个元素
		PostBean post = gson.fromJson(jsonPost, PostBean.class);
		//将文章数据反序列化成文章的各个元素
		Long viewCount = jc.incr("post:"+postID+":view");
		System.out.println("view : "+ viewCount);
		System.out.println(post);
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
		PostBean post = new PostBean("雅虎北研中心宣布将关闭 员工获“N+4”补偿","新浪科技讯 3月18日消息，雅虎高级副总裁今日在内部正式宣布将关闭雅虎北京全球研发中心（以下简称“雅虎北研”）。一位内部员工告诉新浪科技，雅虎将提供给员工“N+4”补偿，“具体的赔偿还需要和各自的管理层谈”。"," 新浪科技","2015年03月18日10:55");
		Long id = insertPostBean(post);
		getPostBean(id);
	}

}
