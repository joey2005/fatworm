package fatworm.engine.predicate;

import fatworm.engine.plan.Plan;
import fatworm.indexing.data.Data;
import fatworm.indexing.table.Record;

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

	@Override
	public Data calc(Record record) {
		// TODO Auto-generated method stub
		return null;
	}
}
