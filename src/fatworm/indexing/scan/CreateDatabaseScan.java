package fatworm.indexing.scan;

import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;
import fatworm.storage.transaction.Transaction;
import fatworm.util.Fatworm;

public class CreateDatabaseScan extends Operation {
	
	private String dbName;
	
	public CreateDatabaseScan(String dbName) {
		this.dbName = dbName;
	}

	@Override
	public void doit() {
		Fatworm.txMap.put(dbName, new Transaction(dbName));
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
