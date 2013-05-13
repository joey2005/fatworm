package fatworm.engine.plan;

import fatworm.indexing.scan.Scan;
import fatworm.indexing.schema.Schema;
import fatworm.engine.predicate.*;

public class SelectPlan extends Plan {
	
	private Plan subPlan;
	private Predicate whereCondition;
	private int planID;
	
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
