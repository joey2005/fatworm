package fatworm.engine.plan;

import fatworm.indexing.scan.Scan;
import fatworm.indexing.schema.Schema;

public class UseDatabasePlan extends Plan {

	private String databaseName;
	private int planID;
	
	public UseDatabasePlan(String databaseName) {
		this.databaseName = databaseName;
	}
	
	@Override
	public String toString() {
		return "Plan #" + (planID = Plan.planCount++) + " <- " + "use database " + databaseName; 
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
