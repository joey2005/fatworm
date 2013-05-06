package fatworm.engine.plan;

import fatworm.indexing.scan.Scan;
import fatworm.engine.predicate.*;

public class DeletePlan extends Plan {
	
	public String tableName;
	public Predicate whereCondition;
	public int planID;
	
	public DeletePlan(String tableName, Predicate whereCondition) {
		this.tableName = tableName;
		this.whereCondition = whereCondition;
	}
	
	@Override
	public String toString() {
		String result = "delete from " + tableName;
		if (whereCondition != null) {
			result += " where ( " + whereCondition.toString() + " )";
		}
		return "Plan #" + (planID = Plan.planCount++) + " <- " + result;
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
