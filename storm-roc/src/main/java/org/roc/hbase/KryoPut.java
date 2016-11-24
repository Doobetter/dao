package org.roc.hbase;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.roc.exception.HBasePutException;

/**
 * HBaseSinkBolt input tuple's format
 * @author LC
 *
 */
public class KryoPut implements Serializable {

	private static final long serialVersionUID = 2959559320344528320L;
	private String tableName = null ;
	private byte[] rowkey = null;
	private ArrayList<KryoColumnValue> columnValues = null;
	public KryoPut(){}
	public KryoPut(String tableName, byte[] rowkey, ArrayList<KryoColumnValue> columnValues ){
		this.tableName = tableName;
		this.rowkey = rowkey;
		this.columnValues = columnValues;
	}
	public KryoPut(byte[] rowkey, ArrayList<KryoColumnValue> columnValues ){
		this.rowkey = rowkey;
		this.columnValues = columnValues;
	}
	/**
	 * 由{@link Put}生成初始化KryoPut对象
	 * @param put
	 */
	public KryoPut(Put put){
		
		this.rowkey = put.getRow();
		//put.getFamilyMap();
		NavigableMap<byte[], List<Cell>> nMap = put.getFamilyCellMap();
		
		Set<byte[]> familys = nMap.keySet();
		ArrayList<KryoColumnValue> list = new ArrayList<KryoColumnValue>();
		for(byte[] f : familys){
			List<Cell> cells = nMap.get(f);
			for (Cell cell :cells){
				KryoColumnValue acv = new KryoColumnValue();
				acv.setFamily(f);
				acv.setQualifier(CellUtil.cloneQualifier(cell));
				acv.setValue(CellUtil.cloneValue(cell));
				list.add(acv);
			}
		}
		this.columnValues = list;		
	
	}
	
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public byte[] getRowkey() {
		return rowkey;
	}
	public void setRowkey(byte[] rowkey) {
		this.rowkey = rowkey;
	}
	public ArrayList<KryoColumnValue> getColumnValues() {
		return columnValues;
	}
	public void setColumnValues(ArrayList<KryoColumnValue> columnValues) {
		this.columnValues = columnValues;
	}
	/**
	 * 将当前对象转换为{@link Put}
	 * @return
	 * @throws HBasePutException
	 */
	public  Put toPut() throws HBasePutException{
		if(this.rowkey == null){
			throw new HBasePutException("PutWrapper object's field rowkey is null ");
		}
		Put put = new Put(this.rowkey);
		ArrayList<KryoColumnValue> list  = this.getColumnValues();
		for (KryoColumnValue column : list){
			put.add(column.getFamily(),column.getQualifier(),column.getValue());
		}
		return put;
	}

}
