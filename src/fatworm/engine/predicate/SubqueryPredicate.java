package fatworm.engine.predicate;

import fatworm.engine.plan.Plan;

public class SubQueryPredicate extends Predicate {
	
	public Plan subPlan;
	
	public SubQueryPredicate(Plan subPlan) {
		this.subPlan = subPlan;
	}
	
	@Override
	public String toString() {
		return subPlan.toString();
	}
}
