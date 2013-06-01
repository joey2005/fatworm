package fatworm.indexing.scan;

public class CreateIndexScan extends Operation {
	
	@SuppressWarnings("unused")
	private String indexName;
	@SuppressWarnings("unused")
	private boolean isUnique;
	@SuppressWarnings("unused")
	private String tableName;
	@SuppressWarnings("unused")
	private String colName;
	
	public CreateIndexScan(String tableName, String indexName, String colName, boolean isUnique) {
		this.tableName = tableName;
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
