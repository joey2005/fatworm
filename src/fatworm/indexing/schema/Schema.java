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
		List<AttributeField> fields = new ArrayList<AttributeField>();
		fields.addAll(attributes);
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
		int posi = colName.indexOf(".");
		for (int i = 0; i < attributes.size(); ++i) {
			String name = attributes.get(i).getColumnName();
			int posj = name.indexOf(".");
			if (posi <= 0) {
				if (posj <= 0) {
					if (name.substring(posj + 1).equals(colName.substring(posi + 1))) {
						return i;
					}
				} else {
					if (name.endsWith(colName.substring(posi + 1))) {
						return i;
					}
				}
			} else {
				if (posj > 0 && name.substring(posj + 1).indexOf(".") > 0) {
					if (name.endsWith(colName)) {
						return i;
					}
				} else {
					if (name.equals(colName)) {
						return i;
					}
				}
			}
		}
		return -1;
	}
}
