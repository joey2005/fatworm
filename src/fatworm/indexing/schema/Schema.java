package fatworm.indexing.schema;

import fatworm.indexing.data.*;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import fatworm.engine.predicate.*;

public class Schema {

	private List<AttributeField> attributes;
	private String tableName;
	
	public Schema(String tblName, List<AttributeField> attributes) {
		this.tableName = tblName;
		this.attributes = attributes;
	}
	
	public Schema union(Schema right, String alias) {// need to change the name of columns?
		List<AttributeField> fields = attributes;
		fields.addAll(right.getAllFields());
		return new Schema(alias, fields);
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public int getColumnCount() {
		return attributes.size();
	}
	
	public List<AttributeField> getAllFields() {
		return attributes;
	}
	
	public AttributeField getFromColumn(int at) {
		return attributes.get(at);
	}

	public AttributeField getFromVariableName(String colName) {
		int index = indexOf(colName);
		if (index >= 0 && index < attributes.size()) {
			return getFromColumn(index);
		}
		return null;
	}
	
	public int indexOf(String colName) {
		int dotpos = colName.indexOf(".");
		String fieldName = colName.substring(dotpos + 1);
		for (int i = 0; i < attributes.size(); ++i) {
			if (dotpos <= 0 && attributes.get(i).getColumnName().endsWith(fieldName) || 
					attributes.get(i).getColumnName().equals(colName)) {
				return i;
			}
		}
		return -1;
	}
}
