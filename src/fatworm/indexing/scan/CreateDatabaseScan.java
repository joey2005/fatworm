package fatworm.indexing.scan;

import fatworm.indexing.LogicalFileMgr;
import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;
import fatworm.storage.file.FileMgr;
import fatworm.util.Fatworm;

public class CreateDatabaseScan extends Operation {
	
	private String dbName;
	
	public CreateDatabaseScan(String dbName) {
		this.dbName = dbName;
	}

	@Override
	public void doit() {
		LogicalFileMgr.createDataBase(dbName);
	}
	
	@Override
	public void close() {
		dbName = null;
	}

	@Override
	public String toString() {
		return "create database scan()";
	}
}
