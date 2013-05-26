package fatworm.indexing.scan;

import fatworm.indexing.LogicalFileMgr;
import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;
import fatworm.storage.file.FileMgr;
import fatworm.util.Fatworm;

public class DropDatabaseScan extends Operation {
	
	private String dbName;
	
	public DropDatabaseScan(String dbName) {
		this.dbName = dbName;
	}
	
	/**
	 * drop a database
	 */
	public void doit() {
		LogicalFileMgr.dropDataBase(dbName);
	}

	@Override
	public void close() {
		dbName = null;
	}

	@Override
	public String toString() {
		return "drop database scan()";
	}

}
