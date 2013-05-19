package fatworm.indexing.schema;

import fatworm.indexing.data.DataType;
import fatworm.engine.predicate.Predicate;

public class AttributeField {
	
	public AttributeField(String colName, DataType type, int isNull, Predicate defaultValue, boolean autoIncrement) {
		this.colName = colName;
		this.type = type;
		this.isNull = isNull;
		if (defaultValue != null) {
			this.isDefault = true;
			this.defaultValue = defaultValue;
		}
		this.autoIncrement = autoIncrement;
	}

	public String getColumnName() {
		return colName;
	}
	
	public DataType getType() {
		return type;
	}
	
	public String colName = null;
	public int isNull = -1;
	public boolean autoIncrement = false;
	public boolean isDefault = false;
	public Predicate defaultValue = null;
	public DataType type = null;

}
