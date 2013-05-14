package fatworm.engine.plan;

import fatworm.indexing.scan.RenameScan;
import fatworm.indexing.scan.Scan;
import fatworm.indexing.schema.Schema;

public class RenamePlan extends Plan {

	private Plan subPlan;
	private Schema schema;
	private String alias;
	private int planID;
	
	public RenamePlan(Plan subPlan, String alias) {
		this.subPlan = subPlan;
		this.alias = alias;
		this.schema = subPlan.getSchema();
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
		return new RenameScan(subPlan.createScan(), schema);
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
