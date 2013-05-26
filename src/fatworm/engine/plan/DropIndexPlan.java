package fatworm.engine.plan;

import fatworm.indexing.scan.DropIndexScan;
import fatworm.indexing.scan.Scan;
import fatworm.indexing.schema.*;

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
		return new DropIndexScan(tableName, indexName);
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
