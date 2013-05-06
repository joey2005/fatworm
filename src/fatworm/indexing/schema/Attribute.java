package fatworm.indexing.schema;

import fatworm.indexing.data.DataType;
import fatworm.engine.predicate.Predicate;

public class Attribute {
	
	public Attribute() {
		colName = null;
		isNull = -1;
		autoIncrement = false;
		isDefault = false;
		defaultValue = null;
		type = null;
	}
	
	public void setColumnName(String colName) {
		this.colName = colName;
	}
	
	public void setType(DataType type) {
		this.type = type;
	}
	
	public void setNull(int isNull) {
		this.isNull = isNull;
	}
	
	public void setDefault(Predicate data) {
		isDefault = true;
		defaultValue = data;
	}
	
	public void setAutoIncrement() {
		autoIncrement = true;
	}
	
	public String getColumnName() {
		return colName;
	}
	
	public DataType getType() {
		return type;
	}
	
	private String colName;
	private int isNull;
	private boolean autoIncrement;
	private boolean isDefault;
	private Predicate defaultValue;
	private DataType type;

}
