package fatworm.engine.plan;

import fatworm.indexing.scan.Scan;

public class DropDatabasePlan extends Plan {
	
	public String databaseName;
	public int planID;
	
	public DropDatabasePlan(String databaseName) {
		this.databaseName = databaseName;
	}
	
	@Override
	public String toString() {
		return "Plan #" + (planID = Plan.planCount++) + " <- " + "drop database " + databaseName;
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
