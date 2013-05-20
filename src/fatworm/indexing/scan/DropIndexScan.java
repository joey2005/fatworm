package fatworm.indexing.scan;

import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;

public class DropIndexScan extends Operation {
	
	private String indexName;
	private String tableName;
	
	public DropIndexScan(String tableName, String indexName) {
		this.tableName = tableName;
		this.indexName = tableName + "_" + indexName;
	}

	@Override
	public void doit() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		indexName = null;
		tableName = null;
	}

	@Override
	public String toString() {
		return "drop index scan()";
	}

}
