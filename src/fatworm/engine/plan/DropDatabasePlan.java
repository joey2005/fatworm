package fatworm.engine.plan;

import fatworm.indexing.scan.DropDatabaseScan;
import fatworm.indexing.scan.Scan;
import fatworm.indexing.schema.Schema;

public class DropDatabasePlan extends Plan {
	
	public String dbName;
	public int planID;
	
	public DropDatabasePlan(String dbName) {
		this.dbName = dbName;
	}
	
	@Override
	public String toString() {
		return "Plan #" + (planID = Plan.planCount++) + " <- " + "drop db " + dbName;
	}

	@Override
	public int getPlanID() {
		return planID;
	}

	@Override
	public Scan createScan() {
		return new DropDatabaseScan(dbName);
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
