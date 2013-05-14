package fatworm.engine.plan;

import fatworm.indexing.scan.CreateIndexScan;
import fatworm.indexing.scan.Scan;
import fatworm.indexing.schema.Schema;

public class CreateIndexPlan extends Plan {
	
	private String indexName;
	private boolean isUnique;
	private String tableName;
	private String colName;
	private int planID;
	
	public CreateIndexPlan(String indexName, boolean isUnique, String tableName, String colName) {
		this.indexName = indexName;
		this.isUnique = isUnique;
		this.tableName = tableName;
		this.colName = colName;
	}
	
	@Override
	public String toString() {
		String result = "create ";
		if (isUnique) {
			result += "unique ";
		}
		result += "index " + indexName + " on " + tableName + "( " + colName + " )";
		result = "Plan #" + (planID = Plan.planCount++) + " <- " + result;
		return result;
	}

	@Override
	public int getPlanID() {
		return planID;
	}

	@Override
	public Scan createScan() {
		return new CreateIndexScan(tableName, indexName, colName, isUnique);
	}

	@Override
	public Plan subPlan() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Schema getSchema() {
		// TODO Auto-generated method stub
		return null;
	}
}
