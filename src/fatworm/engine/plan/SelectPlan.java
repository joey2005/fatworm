package fatworm.engine.plan;

import fatworm.indexing.scan.Scan;
import fatworm.indexing.scan.SelectScan;
import fatworm.indexing.schema.*;
import fatworm.engine.predicate.*;

public class SelectPlan extends Plan {
	
	public Plan subPlan;
	public Schema schema;
	public Predicate whereCondition;
	public int planID;
	
	public SelectPlan(Plan subPlan, Predicate whereCondition) {
		this.subPlan = subPlan;
		this.whereCondition = whereCondition;
		this.schema = subPlan.getSchema();
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
		if (whereCondition == null) {
			return subPlan.createScan();
		} else {
			return new SelectScan(subPlan.createScan(), whereCondition);
		}
	}

	@Override
	public Plan subPlan() {
		return subPlan;
	}

	@Override
	public Schema getSchema() {
		return schema;
	}
}
