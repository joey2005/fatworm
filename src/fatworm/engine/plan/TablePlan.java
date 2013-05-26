package fatworm.engine.plan;

import fatworm.indexing.scan.Scan;
import fatworm.indexing.scan.TableScan;
import fatworm.indexing.schema.*;

public class TablePlan extends Plan {

	public String tableName;
	
	public Schema schema;
	public int planID;
	
	public TablePlan(String tableName, Schema schema) {
		this.tableName = tableName;
		this.schema = schema;
	}
	
	@Override
	public String toString() {
		return "Plan #" + (planID = Plan.planCount++) + " <- " + tableName;
	}

	@Override
	public int getPlanID() {
		return planID;
	}

	@Override
	public Scan createScan() {
		return new TableScan(tableName);
	}

	@Override
	public Plan subPlan() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Schema getSchema() {
		return schema;
	}
}
