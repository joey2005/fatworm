package fatworm.indexing.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import fatworm.indexing.table.Record;

public class TableMgr {

	private ArrayList<ArrayList<Record>> recordTable;
	private HashMap<String, Integer> tableMap;
	
	public TableMgr() {
		recordTable = new ArrayList<ArrayList<Record>>();
		tableMap = new HashMap<String, Integer>();
	}
	
	public void addTable(String tableName) {
		int pos = recordTable.size();
		recordTable.add(new ArrayList<Record>());
		tableMap.put(tableName, pos);
	}
	
	public void addRecord(String tableName, Record record) {
		Integer pos = tableMap.get(tableName);
		ArrayList<Record> table = recordTable.get(pos);
		table.add(record);
	}
	
	public void deleteRecord(String tableName) {
		Integer pos = tableMap.get(tableName);
		recordTable.remove(pos);
		recordTable.add(pos, new ArrayList<Record>());
	}
	
	public void deleteRecord(String tableName, Record record) {
		Integer pos = tableMap.get(tableName);
		ArrayList<Record> table = recordTable.get(pos);
		table.remove(record);
	}
	
	public void updateRecord(String tableName, int index, Record record) {
		Integer pos = tableMap.get(tableName);
		ArrayList<Record> table = recordTable.get(pos);
		table.remove(index);
		table.add(index, record);
	}
	
	public ArrayList<Record> getTable(String tableName) {
		Integer pos = tableMap.get(tableName);
		ArrayList<Record> table = recordTable.get(pos);
		return table;
	}
	
	public Record getRecordFromTable(String tableName, int index) {
		Integer pos = tableMap.get(tableName);
		ArrayList<Record> table = recordTable.get(pos);
		return table.get(index);
	}
	
	public void dropTable(List<String> tableNameList) {
		for (String tableName : tableNameList) {
			Integer pos = tableMap.get(tableName);
			recordTable.remove(pos);
			tableMap.remove(tableName);
		}
	}
}
