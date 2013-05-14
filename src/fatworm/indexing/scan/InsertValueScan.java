package fatworm.indexing.scan;

import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;

public class InsertValueScan extends Operation {
	
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
	public void close() {
		// TODO Auto-generated method stub
		
	}


}
