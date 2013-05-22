package fatworm.engine.plan;

import java.util.HashSet;
import java.util.Set;

import fatworm.indexing.scan.Scan;
import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;

public class SubQueryPlan extends Plan {
	
	public Plan subPlan;
	public Schema schema;
	public int planID;
	
	public SubQueryPlan(Plan subPlan) {
		this.subPlan = subPlan;
		this.schema = subPlan.getSchema();
	}
	
	@Override
	public int getPlanID() {
		return planID;
	}

	@Override
	public Scan createScan() {
		return subPlan.createScan();
	}

	@Override
	public Plan subPlan() {
		return subPlan;
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public String toString() {
		String result = subPlan.toString() + "\n";
		result += "Plan #" + (planID = Plan.planCount++) + "<-" + "(SubQueryPlan)" + subPlan.getPlanID();
		return result;
	}
}
