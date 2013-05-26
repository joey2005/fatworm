package fatworm.indexing;

import java.util.*;

import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;
import fatworm.util.Fatworm;

public class LogicalFileMgr {
	
	public LogicalFileMgr() { }

	public static void createDataBase(String dbName) {
		Fatworm.storageMgr().openDataBase(dbName, true);
	}
	
	public static void dropDataBase(String dbName) {
		Fatworm.storageMgr().dropDataBase(dbName);
	}
	
	public static void useDataBase(String dbName) {
		Fatworm.storageMgr().openDataBase(dbName, false);
	}
	
	public static void createTable(String tableName) {
		
	}
	
	public static void dropTable(List<String> tableNameList) {

	}
	
	public static void addRecord(String tableName, Record record) {

	}
	
	public static void deleteRecord(String tableName) {

	}
	
	public static void deleteRecord(String tableName, Record record) {

	}
	
	public static void updateRecord(String tableName, int index, Record record) {

	}
	
	public static ArrayList<Record> recordTable(String tableName) {

	}
	
	public static Record getRecordFromTable(String tableName, int index) {

	}
	
	public static void addSchema(String tableName, Schema schema) {

	}

	public static Schema getSchema(String tableName) {

	}
}
