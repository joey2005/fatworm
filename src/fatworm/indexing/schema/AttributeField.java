package fatworm.indexing.schema;

import fatworm.indexing.data.Data;
import fatworm.indexing.data.DataType;
import fatworm.indexing.data.IntegerData;
import fatworm.indexing.data.IntegerType;
import fatworm.indexing.data.NumberData;
import fatworm.engine.predicate.Predicate;

public class AttributeField {
	
	public AttributeField(String colName, DataType type, int isNull, Predicate defaultValue, boolean autoIncrement) {
		this.colName = colName;
		this.type = type;
		this.isNull = isNull;
		if (defaultValue != null) {
			this.defaultValue = defaultValue.calc(null);
		}
		this.autoIncrement = autoIncrement;
	}
	
	public AttributeField(String colName, DataType type, int isNull, Data defaultValue, boolean autoIncrement) {
		this.colName = colName;
		this.type = type;
		this.isNull = isNull;
		this.defaultValue = defaultValue;
		this.autoIncrement = autoIncrement;
	}
	
	public AttributeField(String colName, DataType type) {
		this.colName = colName;
		this.type = type;	
	}

	public String getColumnName() {
		return colName;
	}
	
	public Data getDefault() {
		if (defaultValue == null) {
			return type.getDefaultValue();
		}
		return defaultValue;
	}
	
	public Data getAutoIncrement() {
		if (!(type instanceof IntegerType)) {
			try {
				throw new Exception("not a integer type");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		count = NumberData.add((IntegerData)count, IntegerType.ONE, new IntegerType());
		return count;
	}
	
	public DataType getType() {
		return type;
	}
	
	public String colName = null;
	public int isNull = -1;
	public Data count = IntegerType.ZERO;
	public boolean autoIncrement = false;
	public Data defaultValue = null;
	public DataType type = null;

	public static int ONLY_NULL = 1;
	public static int ONLY_NOT_NULL = 0;
	public static int CAN_BE_EVERYTHING = -1;
}
