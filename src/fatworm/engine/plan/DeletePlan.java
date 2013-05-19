package fatworm.engine.plan;

import fatworm.indexing.metadata.InfoMgr;
import fatworm.indexing.scan.DeleteScan;
import fatworm.indexing.scan.Scan;
import fatworm.indexing.schema.Schema;
import fatworm.engine.predicate.*;

public class DeletePlan extends Plan {
	
	public String tableName;
	public Predicate whereCondition;
	public int planID;
	
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
		Scan scan = null;
		Schema schema = InfoMgr.getSchema(tableName);
		if (whereCondition != null) {
			scan = new SelectPlan(new TablePlan(tableName, schema), whereCondition).createScan();
		} else {
			scan = new TablePlan(tableName, schema).createScan();
		}
		return new DeleteScan(tableName, scan);
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
