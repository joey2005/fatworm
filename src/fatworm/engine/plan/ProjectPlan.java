package fatworm.engine.plan;

import fatworm.indexing.scan.Scan;
import fatworm.indexing.schema.Schema;

import java.util.List;

import fatworm.engine.predicate.*;

public class ProjectPlan extends Plan {

	private Plan subPlan;
	private List<Predicate> projectList;
	private List<String> alias;
	private int planID;
	private String groupBy;
	
	public ProjectPlan(Plan subPlan, List<Predicate> pList, List<String> alias, String groupBy) {
		this.subPlan = subPlan;
		this.projectList = pList;
		this.alias = alias;
		this.groupBy = groupBy;
	}
	
	@Override
	public String toString() {
		String result = "( " + subPlan.toString() + " )\n";
		result += "Plan #" + (planID = Plan.planCount++) + " <- " + "Project Plan #" + subPlan.getPlanID() + " into columns( ";
		for (int i = 0; i < projectList.size(); ++i) {
			if (alias.get(i) != null) {
				result += alias.get(i) + ", ";
			} else {
				result += projectList.get(i).toString() + ", ";
			}
		}
		result += ")";
		if (groupBy != null) {
			result += " GROUP BY " + groupBy;
		}
		
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
