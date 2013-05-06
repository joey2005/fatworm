package fatworm.engine.plan;

import fatworm.indexing.scan.Scan;

import java.util.List;

import fatworm.engine.predicate.*;

public class InsertValuePlan extends Plan {
	
	public String tableName;
	public List<Predicate> args;
	public List<String> schema;
	public int planID;
	
	public InsertValuePlan(String tableName, List<Predicate> args, List<String> schema) {
		this.tableName = tableName;
		this.args = args;
		this.schema = schema;
		
		if (this.schema == null) {
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
}
