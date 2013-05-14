package fatworm.engine.plan;

import fatworm.indexing.scan.Scan;
import fatworm.indexing.schema.Schema;

public class InsertSubQueryPlan extends Plan {
	
	private String tableName;
	private Plan subPlan;
	private int planID;
	
	public InsertSubQueryPlan(String tableName, Plan subPlan) {
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

	@Override
	public Plan subPlan() {
		return subPlan;
	}

	@Override
	public Schema getSchema() {
		// TODO Auto-generated method stub
		return null;
	}
}
