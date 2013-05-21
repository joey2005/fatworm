package fatworm.indexing.metadata;

import java.util.ArrayList;
import java.util.HashMap;

import fatworm.indexing.schema.Schema;

public class InfoMgr {
	
	private HashMap<String, Integer> tableMap;
	private ArrayList<Schema> schemaTable;
	
	public InfoMgr() {
		tableMap = new HashMap<String, Integer>();
		schemaTable = new ArrayList<Schema>();
	}
	
	public void addSchema(String tableName, Schema schema) {
		int pos = schemaTable.size();
		schemaTable.add(schema);
		tableMap.put(tableName, pos);
	}

	public Schema getSchema(String tableName) {
		Integer pos = tableMap.get(tableName);
		return schemaTable.get(pos);
	}
}
