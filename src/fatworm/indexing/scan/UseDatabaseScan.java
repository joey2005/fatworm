package fatworm.indexing.scan;

import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;
import fatworm.storage.transaction.Transaction;
import fatworm.util.Fatworm;

public class UseDatabaseScan extends Operation {
	
	private String dbName;
	
	public UseDatabaseScan(String dbName) {
		this.dbName = dbName;
	}
	
	/**
	 * use a database
	 */
	public void doit() {
		Fatworm.tx = Fatworm.txMap.get(dbName);
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
