package fatworm.engine.plan;

import fatworm.indexing.scan.Scan;

public class ProductPlan extends Plan {

	public Plan lhs, rhs;
	public int planID;
	
	public ProductPlan(Plan lhs, Plan rhs) {
		this.lhs = lhs;
		this.rhs = rhs;
	}
	
	@Override
	public String toString() {
		String result = "( " + lhs.toString() + ")\n";
		result += "( " + rhs.toString() + "\n)";
		result += "Plan #" + (planID = Plan.planCount++) + " <- " + "Plan #" + lhs.getPlanID() + " X " + "Plan %" + rhs.getPlanID();
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
}
