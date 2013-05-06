package fatworm.engine.predicate;

import fatworm.engine.plan.Plan;

public class SubqueryPredicate extends Predicate {
	
	public Plan subPlan;
	
	public SubqueryPredicate(Plan subPlan) {
		this.subPlan = subPlan;
	}
	
	@Override
	public String toString() {
		return subPlan.toString();
	}
}
