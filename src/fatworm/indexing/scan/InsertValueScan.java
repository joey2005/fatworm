package fatworm.indexing.scan;

import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;

public class InsertValueScan extends Scan {
	
	private String tableName;
	private Record record;
	
	public InsertValueScan(String tableName, Record record) {
		this.tableName = tableName;
		this.record = record;
	}
	
	/**
	 * insert a record into table
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
