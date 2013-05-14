package fatworm.indexing.schema;

import fatworm.indexing.data.DataType;
import fatworm.engine.predicate.Predicate;

public class Attribute {
	
	public Attribute(String colName, DataType type) {
		this.colName = colName;
		this.type = type;
	}
	
	public Attribute() {
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
	
	private String colName = null;
	private int isNull = -1;
	private boolean autoIncrement = false;
	private boolean isDefault = false;
	private Predicate defaultValue = null;
	private DataType type = null;

}
