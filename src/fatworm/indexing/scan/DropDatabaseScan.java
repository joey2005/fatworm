package fatworm.indexing.scan;

import fatworm.indexing.LogicalFileMgr;

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
