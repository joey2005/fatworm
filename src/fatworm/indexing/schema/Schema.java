package fatworm.indexing.schema;

import fatworm.indexing.data.*;

import java.util.LinkedList;
import java.util.List;

import fatworm.engine.predicate.*;

public class Schema {

	private List<String> primaryKey;
	private List<Attribute> attributes;
	private String tableName;
	
	public Schema() {
		tableName = null;
		primaryKey = new LinkedList<String>();
		attributes = new LinkedList<Attribute>();
	}
	
	public void setTableName(String s) {
		tableName = s;
	}
	
	public void addColumn(Attribute att) {
		attributes.add(att);
	}
	
	public void addPrimaryKey(String s) {
		primaryKey.add(s);
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public int getColumnCount() {
		return attributes.size();
	}
	
	public List<String> getPrimaryKey() {
		return primaryKey;
	}
	
	public List<Attribute> getAttribute() {
		return attributes;
	}
	
	public Attribute getAttributeOf(int at) {
		return attributes.get(at);
	}

}
