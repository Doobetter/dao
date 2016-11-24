package org.roc.hdfs.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.ReflectionUtils;


public class CompressionUtil {

	 public static void decompres(String filename) throws FileNotFoundException, IOException {  
	        System.out.println("[" + new Date() + "] : enter compress");  
	          
	        Configuration conf = new Configuration();  
	        CompressionCodecFactory factory = new CompressionCodecFactory(conf);          
	        CompressionCodec codec = factory.getCodec(new Path(filename));  
	        if (null == codec) {  
	            System.out.println("Cannot find codec for file " + filename);  
	            return;  
	        }  
	          
	        File fout = new File(filename+ ".decoded");  
	        InputStream cin = codec.createInputStream(new FileInputStream(filename));  
	        OutputStream out = new FileOutputStream(fout);  
	          
	        System.out.println("[" + new Date() + "]: start decompressing ");  
	        IOUtils.copyBytes(cin, out, 1024*1024*5, false);  
	        System.out.println("[" + new Date() + "]: decompressing finished ");  
	          
	        cin.close();  
	        out.close();  
	    }  
	
	public void read() throws ClassNotFoundException, IOException{

		
		Class<?> codexClass = Class.forName("");
		//使用指定编码器类来创建一个压缩类，比如GzipCodec编码器

		Configuration  conf = new Configuration();

		CompressionCodec codec = (CompressionCodec)
		ReflectionUtils.newInstance(codexClass,conf);    //创建新的实例

		CompressionOutputStream out = codec.createOutputStream(System.out);//获得压缩好的out

		IOUtils.copyBytes(System.in,out,4096,false);//将输入System.in复制到经过CompressionOutputStream的输出out

		out.finish();
		//结束向压缩流写入数据，但不关闭流    
		
		FileSystem fs = FileSystem.get(conf);
		
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		CompressionCodec codec1 = factory.getCodec(new Path(""));
		
	
		InputStream in1 = fs.open(new Path(""));  
		InputStream in = codec1.createInputStream(in1);
		//codec1.createInputStream(in)
		
		LineReader lr = new LineReader(in,"<hr>".getBytes());
	      
	    Text str = new Text();
	    lr.readLine(str);
		
	}
}
