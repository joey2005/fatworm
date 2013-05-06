package fatworm.engine.plan;

import fatworm.indexing.scan.Scan;

public class CreateIndexPlan extends Plan {
	public String indexName;
	public boolean isUnique;
	public String tableName;
	public String colName;
	public int planID;
	
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
		// TODO Auto-generated method stub
		return null;
	}
}
