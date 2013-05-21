package fatworm.indexing.scan;

import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;
import fatworm.storage.transection.Transection;
import fatworm.util.Fatworm;

public class CreateDatabaseScan extends Operation {
	
	private String dbName;
	
	public CreateDatabaseScan(String dbName) {
		this.dbName = dbName;
	}

	@Override
	public void doit() {

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
