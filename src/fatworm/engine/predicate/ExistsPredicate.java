package fatworm.engine.predicate;

import fatworm.engine.plan.Plan;

public class ExistsPredicate extends Predicate {
	
	boolean neg;
	Plan subPlan;

	public ExistsPredicate(boolean neg, Plan subPlan) {
		this.neg = neg;
		this.subPlan = subPlan;
	}
	
	@Override
	public String toString() {
		String result = subPlan.toString() + "\n";
		result += "Plan %" + subPlan.getPlanID();
		if (neg) {
			result += " not";
		}
		result += " exists";
		return result;
	}
}
