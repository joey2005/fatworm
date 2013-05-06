package fatworm.engine.plan;

import fatworm.indexing.scan.Scan;

public class CreateDatabasePlan extends Plan {
	public String databaseName;
	public int planID;
	
	public CreateDatabasePlan(String databaseName) {
		this.databaseName = databaseName;
	}
	
	@Override
	public String toString() {
		String result = "create database " + databaseName;
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
