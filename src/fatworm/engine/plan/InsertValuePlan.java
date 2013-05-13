package fatworm.engine.plan;

import fatworm.indexing.scan.Scan;
import fatworm.indexing.schema.Schema;

import java.util.List;

import fatworm.engine.predicate.*;

public class InsertValuePlan extends Plan {
	
	private String tableName;
	private List<Predicate> args;
	private int planID;
	
	public InsertValuePlan(String tableName, List<Predicate> args, List<String> columns) {
		this.tableName = tableName;
		this.args = args;
		
		if (columns == null) {
			//read the schema of this table into this.schema
		}
	}
	
	@Override
	public String toString() {
		String result = "insert into " + tableName + ": value( ";
		for (Predicate vp : args) {
			result += vp.toString() + ", ";
		}
		result += " )";
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
