package fatworm.engine.plan;

import fatworm.indexing.scan.Scan;
import fatworm.engine.predicate.*;

public class SelectPlan extends Plan {
	
	public Plan subPlan;
	public Predicate whereCondition;
	public int planID;
	
	public SelectPlan(Plan subPlan, Predicate whereCondition) {
		this.subPlan = subPlan;
		this.whereCondition = whereCondition;
	}
	
	@Override
	public String toString() {
		String result = "( " + subPlan.toString() + ")\n";
		result += "Plan #" + (planID = Plan.planCount++) + " <- " + "select * from Plan #" + subPlan.getPlanID();
		if (whereCondition != null) {
			result += " where ( " + whereCondition.toString() + " )";
		}
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
