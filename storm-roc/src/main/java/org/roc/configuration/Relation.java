package org.roc.configuration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
/**
 *  描述topology各个组件之间的关系，其实就是数据流动关系
 *  有两个子类{@link Dependency} {@link Emit}
 *  streamID 描述数据流的id
 *  fields是数据流tuple的{@link backtype.storm.tuple.Fields.Fields}
 * @author LC
 *
 */
public class Relation implements Serializable {
	private static final long serialVersionUID = -1776880469534638527L;

	protected String streamID;
	protected ArrayList<FieldSchema> fields = new ArrayList<FieldSchema>();

	public Relation() {

	}

	public String getStreamID() {
		return streamID;
	}

	public void setStreamID(String streamID) {
		this.streamID = streamID;
	}

	public ArrayList<FieldSchema> getFields() {
		return fields;
	}

	public void setFields(ArrayList<FieldSchema> fields) {
		this.fields = fields;
	}

	public void addTupleField(FieldSchema field) {
		if (this.fields == null) {
			this.fields = new ArrayList<FieldSchema>();
		}
		this.fields.add(field);
	}

	/**
	 * 定义各个组件的输出数据流
	 * @author LC
	 *
	 */
	public static class Emit extends Relation {
		private static final long serialVersionUID = -8439081317773856197L;

		public Emit(String streamID, ArrayList<FieldSchema> fields) {
			this.streamID = streamID;
			this.fields = fields;
		}
		
		public List<String> getEmitFields(){
			List<String> rs = new ArrayList<String>();
			List<FieldSchema> list = this.getFields();
			for(FieldSchema fs :list){
				rs.add(fs.getFieldName());
			}
			if(rs.size()<=0){
				//默认field就一个
				rs.add("var1");
			}
			return rs;
		}
		
	}

	/**
	 * 定义输入数据流，即依赖的上个节点的数据流信息
	 * @author LC
	 *
	 */
	public static class Dependency extends Relation {
		private static final long serialVersionUID = -2588024050219560454L;
		// 数据流分组的模式，默认为随机的shuffle
		private String streamGrouping = "shuffle";
		private String partitionFields = null;
		private String componentID;

		public Dependency(String componentID, String streamID, ArrayList<FieldSchema> fields) {
			this.componentID = componentID;
			this.streamID = streamID;
			this.fields = fields;
		}

		public Dependency(String componentID, String streamID, String streamGrouping, String partitionFields, ArrayList<FieldSchema> fields) {
			this.componentID = componentID;
			this.streamID = streamID;
			this.streamGrouping = streamGrouping;
			this.partitionFields = partitionFields;
			this.fields = fields;
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof Relation)) {
				return false;
			}
			Dependency d = (Dependency) obj;
			if (this.componentID.equals(d.getComponentID()) && this.streamID.equals(d.getStreamID())) {
				return true;
			}
			return false;
		}

		public String getStreamGrouping() {
			return streamGrouping;
		}

		public void setStreamGrouping(String streamGrouping) {
			this.streamGrouping = streamGrouping;
		}

		public String getPartitionFields() {
			return partitionFields;
		}

		public void setPartitionFields(String partitionFields) {
			this.partitionFields = partitionFields;
		}

		public String getComponentID() {
			return componentID;
		}

		public void setComponentID(String componentID) {
			this.componentID = componentID;
		}

	}
}
