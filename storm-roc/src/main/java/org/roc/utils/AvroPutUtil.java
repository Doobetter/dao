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
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.roc.hbase.AvroColumnValue;
import org.roc.hbase.AvroPut;
/**
 * AvroPut的工具类
 * 重复使用 ByteArrayOutputStream对象、 DatumWriter对象、DatumReader对象、Schema对象、Encoder对象
 * @author LC
 *
 */
public class AvroPutUtil {
	private ByteArrayOutputStream out = null;
	private DatumWriter<AvroPut> datumWriter = null;	
	private DatumReader<AvroPut> datumReader = null;
	private Schema schema = null;
	private Encoder encoder = null;
	// for reuse
	//private AvroPut resue= new AvroPut();
	public AvroPutUtil(){
		this.out = new ByteArrayOutputStream();
		this.datumWriter = new SpecificDatumWriter<AvroPut>(AvroPut.class);	
		this.schema = AvroPut.SCHEMA$;
		this.datumWriter.setSchema(schema);
		//encoder可以将数据写入流中，binaryEncoder第二个参数是重用的encoder，这里不重用，所用传空
		this.encoder = EncoderFactory.get().binaryEncoder(out, null);
		
		this.datumReader = new SpecificDatumReader<AvroPut>(AvroPut.class);	
	}
	//AvroPut转化为Put
	public  Put avroPutToPut(AvroPut avroPut){
		Put put = new Put(avroPut.getRow().array());
		List<AvroColumnValue> list  = avroPut.getColumnValues();
		for (AvroColumnValue column : list){
			put.add(column.getFamily().array(),column.getQualifier().array(),column.getValue().array());
		}
		return put;
	}
	//Put转化为AvroPut
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
	
	// 需要改善   会不会影响性能，耗内存？
	public  byte[] avroPutToBytes(AvroPut aPut) throws IOException{
		out.reset();
		this.datumWriter.write(aPut, encoder);		
        this.encoder.flush(); 
        byte [] rs = this.out.toByteArray();
        return  rs;

	}
	
	public AvroPut bytesToAvroPut(byte[] bytes) throws IOException{
		Decoder decoder=DecoderFactory.get().binaryDecoder(bytes,null);
		AvroPut t=datumReader.read(null,decoder);
		return t;
	}
	
	
	public  <T extends SpecificRecordBase> byte[] getByteArrayFromAvroObj(T t, Class<T> clazz) throws IOException{
		
		//ByteArrayOutputStream out = new ByteArrayOutputStream();
		out.reset();
		DatumWriter<T> datumWriter = new SpecificDatumWriter<T>(clazz);		
		Schema schema = t.getSchema();
		datumWriter.setSchema(schema);
		datumWriter.write(t, encoder);		
        encoder.flush();
        //out.close();
        return  out.toByteArray();

	}
	public  static <T extends SpecificRecordBase> T getAvroObjFromByteArray(Class<T> clazz,byte[] byteArr) throws IOException{
		
		DatumReader<T> datumReader = new SpecificDatumReader<T>(clazz);	
		
		Decoder decoder=DecoderFactory.get().binaryDecoder(byteArr,null);
		
		T t=datumReader.read(null,decoder);
		
		return t;
		
	}
	
	
	
	public void close(){
		try {
			this.out.close();
		} catch (IOException e) {
			
		}
		
	}
	public static void main(String []args ) throws IOException{
		// 测试
		AvroPutUtil util = new AvroPutUtil();
		Put put = new Put(Bytes.toBytes("001"));
		put.add(Bytes.toBytes("a"), Bytes.toBytes("name"), Bytes.toBytes("liucheng"));
		AvroPut avroPut = util.putToAvroPut(put);
		
		byte[] bytes = util.avroPutToBytes(avroPut);
		System.out.println(Bytes.toString(bytes));
		System.out.println(bytes.length);
		System.out.println(Bytes.toString(util.bytesToAvroPut(bytes).getRow().array()));
		
		//第二个 复用
		put = new Put(Bytes.toBytes("002"));
		put.add(Bytes.toBytes("a"), Bytes.toBytes("name"), Bytes.toBytes("liucheng221212"));
		avroPut = util.putToAvroPut(put);
		bytes = util.avroPutToBytes(avroPut);
		System.out.println(Bytes.toString(bytes));
		System.out.println(bytes.length);
		System.out.println(Bytes.toString(util.bytesToAvroPut(bytes).getRow().array()));
		
	}
}
