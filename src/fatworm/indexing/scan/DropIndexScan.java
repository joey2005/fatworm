package fatworm.indexing.scan;

public class DropIndexScan extends Operation {
	
	@SuppressWarnings("unused")
	private String indexName;
	@SuppressWarnings("unused")
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
