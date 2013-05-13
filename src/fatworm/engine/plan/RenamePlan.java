package fatworm.engine.plan;

import fatworm.indexing.scan.Scan;
import fatworm.indexing.schema.Schema;

public class RenamePlan extends Plan {

	private Plan subPlan;
	private String alias;
	private int planID;
	
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
