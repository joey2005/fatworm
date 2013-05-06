package fatworm.engine.plan;

import fatworm.indexing.scan.Scan;

import java.util.List;

import fatworm.engine.predicate.*;

public class UpdatePlan extends Plan {
	
	public String tableName;
	public List<String> colNameList;
	public List<Predicate> valueList;
	public Predicate whereCondition;
	public int planID;
	
	public UpdatePlan(String tableName, List<String> colNameList, List<Predicate> valueList, Predicate whereCondition2) {
		this.tableName = tableName;
		this.colNameList = colNameList;
		this.valueList = valueList;
		this.whereCondition = whereCondition2;
	}
	
	@Override
	public String toString() {
		String result = "update " + tableName + " ";
		for (int i = 0; i < colNameList.size(); ++i) {
			result += "set " + colNameList.get(i) + " = " + valueList.get(i).toString() + "\n";
		}
		if (whereCondition != null) {
			result += "( " + whereCondition.toString() + " )";
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
}
