package fatworm.indexing.scan;

import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;

public class CreateDatabaseScan extends Operation {
	
	private String dbName;
	
	public CreateDatabaseScan(String dbName) {
		this.dbName = dbName;
	}
	
	/**
	 * create a new database
	 */
	public void doit() {
		
	}

	@Override
	public void close() {
		dbName = null;
	}

}
