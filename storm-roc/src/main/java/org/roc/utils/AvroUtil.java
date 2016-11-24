package org.roc.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.roc.hbase.AvroColumnValue;
import org.roc.hbase.AvroPut;



public class AvroUtil implements ISerializationUtil {
	private ByteArrayOutputStream out = null;
	private DatumWriter datumWriter = null;	
	private DatumReader datumReader = null;
	private Schema schema = null;
	private Encoder encoder = null;
	
	public <T> AvroUtil(Class<T> clazz){
		this.out = new ByteArrayOutputStream();
		this.datumWriter = new SpecificDatumWriter<T>(clazz);	
	    try {
	    	this.schema = (Schema)clazz.getField("SCHEMA$").get(null);
		} catch (Exception e) {
			
		}
		this.datumWriter.setSchema(schema);
		//encoder可以将数据写入流中，binaryEncoder第二个参数是重用的encoder，这里不重用，所用传空
		this.encoder = EncoderFactory.get().binaryEncoder(out, null);
		this.datumReader = new SpecificDatumReader<T>(clazz);	
	}
	@Override
	public <T> byte[] toBytes(T t) {
		out.reset();
		try {
			this.datumWriter.write(t, encoder);
			this.encoder.flush(); 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
       
        byte [] rs = this.out.toByteArray();
        return  rs;
	}

	@Override
	public <T> T toObject(byte[] bs, Class<T> clazz) {

		Decoder decoder=DecoderFactory.get().binaryDecoder(bs,null);
		T t = null;
		try {
			t = (T) datumReader.read(null,decoder);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return t;
		
	}
	
	public  Put avroPutToPut(AvroPut avroPut){
		Put put = new Put(avroPut.getRow().array());
		List<AvroColumnValue> list  = avroPut.getColumnValues();
		for (AvroColumnValue column : list){
			put.add(column.getFamily().array(),column.getQualifier().array(),column.getValue().array());
		}
		return put;
	}
	
	
	public <T> Put toPut(T t){
		Put put = new Put(((AvroPut)t).getRow().array());
		List<AvroColumnValue> list  = ((AvroPut)t).getColumnValues();
		for (AvroColumnValue column : list){
			put.add(column.getFamily().array(),column.getQualifier().array(),column.getValue().array());
		}
		return put;
		
	}
	
	
	public  AvroPut putToAvroPut(Put put){
		AvroPut avroPut = new AvroPut();
		avroPut.setRow(ByteBuffer.wrap(put.getRow()));
		
		List<AvroColumnValue> list = new ArrayList<AvroColumnValue>();
		
		//put.getFamilyMap();
		NavigableMap<byte[], List<Cell>> nMap = put.getFamilyCellMap();
		
		Set<byte[]> familys = nMap.keySet();
	
		for(byte[] f : familys){
			List<Cell> cells = nMap.get(f);
			for (Cell cell :cells){
				AvroColumnValue acv = new AvroColumnValue();
				acv.setFamily(ByteBuffer.wrap(f));
				acv.setQualifier(ByteBuffer.wrap(CellUtil.cloneQualifier(cell)));
				acv.setValue(ByteBuffer.wrap(CellUtil.cloneValue(cell)));
				list.add(acv);
			}
		}
		
		avroPut.setColumnValues(list);
		return avroPut;
	}
	
	
	public static void main(String []args) throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
		AvroUtil util = new AvroUtil(AvroPut.class);
		Put put = new Put(Bytes.toBytes("001"));
		put.add(Bytes.toBytes("a"), Bytes.toBytes("name"), Bytes.toBytes("liucheng"));
		AvroPut avroPut = util.putToAvroPut(put);
		
		byte[] bytes = util.toBytes(avroPut);
		System.out.println(Bytes.toString(bytes));
		System.out.println(bytes.length);
		System.out.println(Bytes.toString(util.toObject(bytes, AvroPut.class).getRow().array()));
		
		//第二个 复用
		put = new Put(Bytes.toBytes("002"));
		put.add(Bytes.toBytes("a"), Bytes.toBytes("name"), Bytes.toBytes("liucheng221212"));
		avroPut = util.putToAvroPut(put);
		bytes = util.toBytes(avroPut);
		System.out.println(Bytes.toString(bytes));
		System.out.println(bytes.length);
		System.out.println(Bytes.toString(util.toObject(bytes, AvroPut.class).getRow().array()));

	}

}
