package fatworm.engine.plan;

import fatworm.indexing.scan.InsertSubQueryScan;
import fatworm.indexing.scan.Scan;
import fatworm.indexing.schema.*;

public class InsertSubQueryPlan extends Plan {
	
	public String tableName;
	public Schema schema;
	public Plan subPlan;
	public int planID;
	
	public InsertSubQueryPlan(String tableName, Plan subPlan) {
		this.tableName = tableName;
		this.subPlan = subPlan;
		this.schema = subPlan.getSchema();
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
		return new InsertSubQueryScan(tableName, subPlan.createScan());
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
