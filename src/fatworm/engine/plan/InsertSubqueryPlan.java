package fatworm.engine.plan;

import fatworm.indexing.scan.Scan;

public class InsertSubqueryPlan extends Plan {
	
	public String tableName;
	public Plan subPlan;
	public int planID;
	
	public InsertSubqueryPlan(String tableName, Plan subPlan) {
		this.tableName = tableName;
		this.subPlan = subPlan;
	}
	
	@Override
	public String toString() {
		String result = " ( " + subPlan.toString() + ")\n";
		result += "Plan #" + (planID = Plan.planCount++) + " <- " + "insert into " + tableName + " ResultSet of Plan #" + subPlan.getPlanID();
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
