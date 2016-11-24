package org.roc.configuration;

import java.io.Serializable;

/**
 * 用以描述 {@link backtype.storm.tuple.Fields} 的各个field
 * @author LC
 *
 */
public class FieldSchema implements Serializable {
	private static final long serialVersionUID = 6932525424766831349L;
	/**
	 *  field 的 name,在declareOutputFields时需要用
	 */
	private String fieldName ;
	/**
	 * field的类型
	 */
	private FieldSchemaType type;
	/**
	 * field的类名， 对于String类型的数据clazzName存储其字段分隔符
	 */
	private String clazzName;

	//private static Gson gson = new Gson();
	
	public FieldSchema() {
	}

	/**
	 * 如果 {@link FieldSchemaType}中有typeName，clazzName会使用的 {@link FieldSchemaType}getClazzName()
	 * @param fieldName
	 * @param typeName
	 * @param clazzName
	 */
	public FieldSchema(String fieldName , String typeName, String clazzName) {
		this.fieldName = fieldName;
		if(typeName!=null){
			//已有type，或者有type参数
			FieldSchemaType[] types = FieldSchemaType.class.getEnumConstants();
			for (FieldSchemaType t : types) {
				if (t.getTypeName().equals(typeName)) {
					this.type = t;
					if (t.getClazzName() != null) {
						this.clazzName = t.getClazzName();
					} else {
						this.clazzName = clazzName;
					}
					break;
				}
			}
		}else{
			//未知type，无type参数
			this.type = null;
			this.clazzName =clazzName;
		}
		
	}

	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	public FieldSchemaType getType() {
		return type;
	}

	public void setType(FieldSchemaType type) {
		this.type = type;
	}

	public String getClazzName() {
		return clazzName;
	}

	public void setClazzName(String clazzName) {
		this.clazzName = clazzName;
	}

	// for String data,xml配置文件中的转义字符要特殊处理
	public String getTupleFiledDelimer() {
		String delimer = this.getClazzName();
		if (TopologyConfiguration.ASCII_CONTROL_MAP.containsKey(delimer)) {
			return TopologyConfiguration.ASCII_CONTROL_MAP.get(delimer);
		} else {
			return delimer;
		}
	}

	/*public String toJSON(){
		return gson.toJson(this, this.getClass());
	}
	public static FieldSchema getFromJSON(String json){
		return gson.fromJson(json, FieldSchema.class);
	}*/
}