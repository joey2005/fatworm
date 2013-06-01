package fatworm.indexing.scan;

import fatworm.indexing.LogicalFileMgr;

public class UseDatabaseScan extends Operation {
	
	private String dbName;
	
	public UseDatabaseScan(String dbName) {
		this.dbName = dbName;
	}
	
	/**
	 * use a database
	 */
	public void doit() {
		LogicalFileMgr.useDataBase(dbName);
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String toString() {
		return "use database scan()";
	}

}
