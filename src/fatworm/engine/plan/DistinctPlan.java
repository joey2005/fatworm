package fatworm.engine.plan;

import fatworm.indexing.scan.DistinctScan;
import fatworm.indexing.scan.Scan;
import fatworm.indexing.schema.*;

public class DistinctPlan extends Plan {
	
	public Plan subPlan;
	public int planID;
	public Schema schema;
	
	public DistinctPlan(Plan subPlan) {
		this.subPlan = subPlan;
		this.schema = subPlan.getSchema();
	}
	
	@Override
	public String toString() {
		String result = "( " + subPlan + " )\n";
		result += "Plan #" + (planID = Plan.planCount++) + " <- " + "make Plan #" + subPlan.getPlanID() + " unique! ";
		return result;
	}

	@Override
	public int getPlanID() {
		return planID;
	}

	@Override
	public Scan createScan() {
		return new DistinctScan(subPlan.createScan());
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
