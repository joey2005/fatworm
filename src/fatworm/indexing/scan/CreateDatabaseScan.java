package fatworm.indexing.scan;

import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;

public class CreateDatabaseScan extends Scan {
	
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
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Record next() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Schema getSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void beforeFirst() {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
