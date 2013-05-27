package fatworm.indexing.metadata;

import fatworm.indexing.schema.*;
import fatworm.indexing.table.TableFile;

import java.util.*;

public class MetadataMgr {

	private static TableMgr tableMgr;
	private static IndexMgr indexMgr;
	
	public MetadataMgr(boolean isNew) {
		tableMgr = new TableMgr(isNew);
		indexMgr = new IndexMgr(isNew, tableMgr);
	}
	
	public void createTable(String tableName, Schema schema) {
		tableMgr.createTable(tableName, schema);
	}
	
	public TableInfo getTableInfo(String tableName) {
		return tableMgr.getTableInfo(tableName);
	}
	
	public TableFile getTableAccess(String tableName) {
		return tableMgr.getTableFileAccess(tableName);
	}
	
	public void dropTable(List<String> tableNameList) {
		tableMgr.dropTable(tableNameList);
	}
	
	public void dropAll() {
		tableMgr.dropAll();
	}
	
	public void createIndex(String indexName, String tableName, String fieldName) {
		indexMgr.createIndex(indexName, tableName, fieldName);
	}
	
	public Map<String, IndexInfo> getIndexInfo(String tableName) {
		return indexMgr.getIndexInfo(tableName);
	}
	
}
