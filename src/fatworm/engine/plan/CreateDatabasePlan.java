package fatworm.engine.plan;

import fatworm.indexing.scan.Scan;
import fatworm.indexing.schema.Schema;

public class CreateDatabasePlan extends Plan {
	
	public String dbName;
	public int planID;
	
	public CreateDatabasePlan(String dbName) {
		this.dbName = dbName;
	}
	
	@Override
	public String toString() {
		String result = "create database " + dbName;
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
