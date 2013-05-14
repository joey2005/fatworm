package fatworm.indexing.schema;

import fatworm.indexing.data.*;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import fatworm.engine.predicate.*;

public class Schema {

	private List<Attribute> attributes;
	private String tableName;
	
	public Schema() {
		tableName = null;
		attributes = new ArrayList<Attribute>();
	}
	
	public Schema(String tblName, List<Attribute> attributes) {
		this.tableName = tblName;
		this.attributes = attributes;
	}
	
	public Schema union(Schema right, String alias) {// need to change the name of columns?
		List<Attribute> fields = attributes;
		fields.addAll(right.getAllFields());
		return new Schema(alias, fields);
	}
	
	public void setTableName(String s) {
		tableName = s;
	}
	
	public void addColumn(Attribute att) {
		attributes.add(att);
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public int getColumnCount() {
		return attributes.size();
	}
	
	public List<Attribute> getAllFields() {
		return attributes;
	}
	
	public Attribute getFields(int at) {
		return attributes.get(at);
	}

	public Attribute getFields(String colName) {
		for (Attribute attr : attributes) {
			if (attr.getColumnName().equals(colName)) {
				return attr;
			}
		}
		return null;
	}
	
}
