package fatworm.engine.plan;

import fatworm.indexing.scan.Scan;

public class TablePlan extends Plan {

	String tableName;
	public int planID;
	
	public TablePlan(String tableName) {
		this.tableName = tableName;
	}
	
	@Override
	public String toString() {
		return "Plan #" + (planID = Plan.planCount++) + " <- " + tableName;
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
