package fatworm.engine.plan;

import fatworm.indexing.scan.Scan;

public class RenamePlan extends Plan {

	public Plan subPlan;
	public String alias;
	public int planID;
	
	public RenamePlan(Plan subPlan, String alias) {
		this.subPlan = subPlan;
		this.alias = alias;
	}
	
	@Override
	public String toString() {
		String result = "( " + subPlan.toString() + " )\n";
		result += "Plan #" + (planID = Plan.planCount++) + " <- " + "Rename " + "Plan #" + subPlan.getPlanID() + " as " + alias;
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
