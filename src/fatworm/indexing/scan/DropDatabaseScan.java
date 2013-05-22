package fatworm.indexing.scan;

import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;
import fatworm.storage.transaction.Transaction;
import fatworm.util.Fatworm;

public class DropDatabaseScan extends Operation {
	
	private String dbName;
	
	public DropDatabaseScan(String dbName) {
		this.dbName = dbName;
	}
	
	/**
	 * drop a database
	 */
	public void doit() {
		if (Fatworm.txMap.containsKey("dbName")) {
			Transaction tx = Fatworm.txMap.get(dbName);
			if (Fatworm.tx == tx) {
				Fatworm.tx = null;
			}
			Fatworm.txMap.remove(dbName);
		}
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
