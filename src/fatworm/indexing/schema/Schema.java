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
		for (AttributeField attr : attributes) {
			if (attr.getColumnName().equals(colName)) {
				return attr;
			}
		}
		return null;
	}
	
	public int indexOf(String colName) {
		for (int i = 0; i < attributes.size(); ++i) {//System.out.println(attributes.get(i).getColumnName());
			if (attributes.get(i).getColumnName().equals(colName)) {
				return i;
			}
		}
		return -1;
	}
}
