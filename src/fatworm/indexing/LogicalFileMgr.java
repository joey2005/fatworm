package fatworm.indexing;

import java.util.List;

import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.TableFile;
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
	
	public static TableFile createTable(String tableName, Schema schema) {
		Fatworm.metadataMgr().createTable(tableName, schema);
		TableFile result = Fatworm.metadataMgr().getTableAccess(tableName);
		return result;
	}
	
	public static void dropTable(List<String> tableNameList) {
		Fatworm.metadataMgr().dropTable(tableNameList);
	}
	
	public static Schema getSchema(String tableName) {
		return Fatworm.metadataMgr().getTableInfo(tableName).schema();
	}
}
