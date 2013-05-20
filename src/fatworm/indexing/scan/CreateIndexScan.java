package fatworm.indexing.scan;

import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;

public class CreateIndexScan extends Operation {
	
	private String indexName;
	private boolean isUnique;
	private String tableName;
	private String colName;
	
	public CreateIndexScan(String tableName, String indexName, String colName, boolean isUnique) {
		this.tableName = this.tableName;
		this.indexName = tableName + "_" + indexName;
		this.colName = colName;
		this.isUnique = isUnique;
	}

	@Override
	public void doit() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		indexName = null;
		tableName = null;
		colName = null;
	}

	@Override
	public String toString() {
		return "create index scan()";
	}

}
