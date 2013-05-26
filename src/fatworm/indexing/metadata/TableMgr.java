package fatworm.indexing.metadata;

import java.util.*;

import fatworm.indexing.LogicalFileMgr;
import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;

public class TableMgr {

	private List<ArrayList<Record>> recordTable;
	private Map<String, Integer> tableMap;
	private List<Schema> schemaTable;
	
	public TableMgr(boolean isnew) {		
		recordTable = new ArrayList<ArrayList<Record>>();
		tableMap = new HashMap<String, Integer>();
		schemaTable = new ArrayList<Schema>();
		
		if (!isnew) {
			
		}
	}
	
	public void createTable(String tableName, Schema schema) {
		int pos = recordTable.size();
		recordTable.add(new ArrayList<Record>());
		tableMap.put(tableName, pos);
	}
	
	public TableInfo getTableInfo(String tableName) {
		Schema schema = LogicalFileMgr.getSchema(tableName);
		return new TableInfo(tableName, schema);
	}
	
}
