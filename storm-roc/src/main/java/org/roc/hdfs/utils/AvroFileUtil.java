package org.roc.hdfs.utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;



import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class AvroFileUtil {
	
	private static final Logger logger = LoggerFactory.getLogger(AvroFileUtil.class);
	
	/*
	 * 创建arvo Writer 句柄
	 */
	public final static <T extends SpecificRecordBase> DataFileWriter<T> getDataFileWriter(T t,Class<T> clazz,OutputStream FSOutput) throws Exception {
		
		DatumWriter<T> datumWriter = new SpecificDatumWriter<T>(clazz);
		DataFileWriter<T> dataFileWriter =  null;
		//dataFileWriter	= new DataFileWriter<T>(datumWriter).setSyncInterval(2000);
		dataFileWriter	= new DataFileWriter<T>(datumWriter);
		
		//dataFileWriter.setCodec(CodecFactory.snappyCodec());
		Schema schema = t.getSchema();
		
		try {	
			
			dataFileWriter.create(schema, FSOutput);
		} catch (Exception e) {
	
			throw new Exception(e);
		}
		
		return dataFileWriter;
	}
	/*
	 * 创建arvo Writer句柄,存入本地文件
	 */
	public final static <T extends SpecificRecordBase> DataFileWriter<T> getDataFileWriter(T t,Class<T> clazz,File file) throws Exception {
		
		DatumWriter<T> datumWriter = new SpecificDatumWriter<T>(clazz);
		DataFileWriter<T> dataFileWriter =  null;
		//dataFileWriter	= new DataFileWriter<T>(datumWriter).setSyncInterval(100);
		dataFileWriter	= new DataFileWriter<T>(datumWriter);
		
		//dataFileWriter.setCodec(CodecFactory.snappyCodec());
		
		Schema schema = t.getSchema();
		
		try {	
			
			dataFileWriter.create(schema, file);
		} catch (Exception e) {
	
			throw new Exception(e);
		}
		
		return dataFileWriter;
	}
	

	public final static <T extends SpecificRecordBase> DataFileReader<T> getDataFileReader(Class<T> clazz,File file) throws Exception {
	
		DatumReader<T> datumReader = new SpecificDatumReader<T>(clazz);
		DataFileReader<T> dataFileReader = new DataFileReader<T>(file, datumReader);
		
		
		return dataFileReader;
	}

	public final static <T extends SpecificRecordBase> DataFileWriter<T> getDataFileWriterFromFSOutput(T t,Class<T> clazz,OutputStream FSOutput) throws Exception {
		
			DatumWriter<T> datumWriter = new SpecificDatumWriter<T>(clazz);
			DataFileWriter<T> dataFileWriter =  null;
			//dataFileWriter	= new DataFileWriter<T>(datumWriter).setSyncInterval(100);
			dataFileWriter	= new DataFileWriter<T>(datumWriter);
			
			//dataFileWriter.setCodec(CodecFactory.snappyCodec());
			Schema schema = t.getSchema();
			
			try {				
				dataFileWriter.create(schema, FSOutput);
			} catch (Exception e) {
				logger.error("create avro dataFileWriter is error! \nError:"+e.getMessage());
				throw new Exception(e);
			}
			
			return dataFileWriter;
	}
	
	/*
	 * 关闭文件
	 */
	public final static  void closeFile(OutputStream FSOutput,DataFileWriter dataFileWriter){
		try {
		if(dataFileWriter != null){	
			dataFileWriter.close();
		}
		} catch (IOException e) {
			logger.error("avro file close,error!\n"+e.getMessage());
		}finally{
			   IOUtils.cleanup(null, FSOutput);
		}
		
	}
	
	
	
	/*
	 * 
	 */
	public final static <T extends SpecificRecordBase> byte[] getByteArrayFromAvroObj(T t,Class<T> clazz) throws IOException{
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		
		DatumWriter<T> datumWriter = new SpecificDatumWriter<T>(clazz);		
			
		Schema schema = t.getSchema();
		
		datumWriter.setSchema(schema);
		
		//encoder可以将数据写入流中，binaryEncoder第二个参数是重用的encoder，这里不重用，所用传空
		Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		
		datumWriter.write(t, encoder);		
        encoder.flush();
        out.close();
       return  out.toByteArray();
		
	}
	
	/*
	 * 
	 */
	public final static <T extends SpecificRecordBase> T getAvroObjFromByteArray(Class<T> clazz,byte[] byteArr) throws IOException{
		
		DatumReader<T> datumReader = new SpecificDatumReader<T>(clazz);	
		
		Decoder decoder=DecoderFactory.get().binaryDecoder(byteArr,null);
		
		T t=datumReader.read(null,decoder);
		
		return t;
		
	}
	
	
}
