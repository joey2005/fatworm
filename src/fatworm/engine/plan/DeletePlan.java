package fatworm.engine.plan;

import fatworm.indexing.scan.Scan;
import fatworm.indexing.schema.Schema;
import fatworm.engine.predicate.*;

public class DeletePlan extends Plan {
	
	private String tableName;
	private Predicate whereCondition;
	private int planID;
	
	public DeletePlan(String tableName, Predicate whereCondition) {
		this.tableName = tableName;
		this.whereCondition = whereCondition;
	}
	
	@Override
	public String toString() {
		String result = "delete from " + tableName;
		if (whereCondition != null) {
			result += " where ( " + whereCondition.toString() + " )";
		}
		return "Plan #" + (planID = Plan.planCount++) + " <- " + result;
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Schema getSchema() {
		// TODO Auto-generated method stub
		return null;
	}
}
