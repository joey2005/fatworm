package fatworm.engine.plan;

import fatworm.indexing.scan.Scan;
import fatworm.indexing.schema.Schema;

public class DistinctPlan extends Plan {
	
	private Plan subPlan;
	private int planID;
	
	public DistinctPlan(Plan subPlan) {
		this.subPlan = subPlan;
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
