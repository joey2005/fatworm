package fatworm.engine.plan;

import java.util.HashSet;
import java.util.Set;

import fatworm.indexing.scan.Scan;
import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;

public class SubQueryPlan extends Plan {
	
	private Plan subPlan;
	private Schema schema;
	private Record record;
	private Scan scan;
	private int planID;
	
	public SubQueryPlan(Plan subPlan) {
		this.subPlan = subPlan;
		schema = subPlan.getSchema();
		record = null;
		scan = null;
	}
	
	public void remove() {
		if (scan != null) {
			scan.close();
		}
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
		return schema;
	}

	@Override
	public String toString() {
		String result = subPlan.toString() + "\n";
		result += "Plan #" + (planID = Plan.planCount++) + "<-" + "(SubQueryPlan)" + subPlan.getPlanID();
		return result;
	}
}
