package fatworm.engine.plan;

import fatworm.indexing.scan.Scan;

public class DropIndexPlan extends Plan {

	public String indexName;
	public String tableName;
	public int planID;
	
	public DropIndexPlan(String indexName, String tableName) {
		this.indexName = indexName;
		this.tableName = tableName;
	}
	
	@Override
	public String toString() {
		return "Plan #" + (planID = Plan.planCount++) + " <- " + "drop index " + indexName + " from " + tableName;
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
