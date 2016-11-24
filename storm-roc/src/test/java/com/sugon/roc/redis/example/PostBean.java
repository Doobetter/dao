package com.sugon.roc.redis.example;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class PostBean implements Serializable {
	//博客文章是由标题、正文、作者与发布时间等多个元素构成的
	private String title;
	private String content;
	private String author;
	private String time;
	
	public PostBean(){}
	public PostBean(String title, String content, String author, String time){
		this.title = title;
		this.content = content;
		this.author = author;
		this.time = time;
	}
	
	
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	public String getAuthor() {
		return author;
	}
	public void setAuthor(String author) {
		this.author = author;
	}
	public String getTime() {
		return time;
	}
	public void setTime(String time) {
		this.time = time;
	}
	@Override
	public String toString() {
		return "[title=" + title + ", content=" + content
				+ ", author=" + author + ", time=" + time + "]";
	}
	
	public Map<String,String> toMap(){
		HashMap<String,String> map = new HashMap<String,String>();
		map.put("title", this.title);
		map.put("content", this.content);
		map.put("author", this.author);
		map.put("time", this.time);
		return map;
	}
}
