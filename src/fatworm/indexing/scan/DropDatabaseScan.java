package fatworm.indexing.scan;

import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;

public class DropDatabaseScan extends Operation {
	
	private String dbName;
	
	public DropDatabaseScan(String dbName) {
		this.dbName = dbName;
	}
	
	/**
	 * drop a database
	 */
	public void doit() {
		
	}

	@Override
	public void close() {
		dbName = null;
	}

}
