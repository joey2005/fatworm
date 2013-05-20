package fatworm.indexing.scan;

import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;

public class CreateDatabaseScan extends Operation {
	
	private String dbName;
	
	public CreateDatabaseScan(String dbName) {
		this.dbName = dbName;
	}

	@Override
	public void doit() {
		// TODO Auto-generated method stub
		
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
