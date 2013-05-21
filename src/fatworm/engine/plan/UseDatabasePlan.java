package fatworm.engine.plan;

import fatworm.indexing.scan.Scan;
import fatworm.indexing.scan.UseDatabaseScan;
import fatworm.indexing.schema.Schema;

public class UseDatabasePlan extends Plan {

	public String dbName;
	public int planID;
	
	public UseDatabasePlan(String dbName) {
		this.dbName = dbName;
	}
	
	@Override
	public String toString() {
		return "Plan #" + (planID = Plan.planCount++) + " <- " + "use database " + dbName; 
	}

	@Override
	public int getPlanID() {
		return planID;
	}

	@Override
	public Scan createScan() {
		return new UseDatabaseScan(dbName);
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
