package fatworm.engine.predicate;

import fatworm.engine.plan.Plan;
import fatworm.indexing.data.Data;
import fatworm.indexing.table.Record;

public class InPredicate extends Predicate {
	
	public Predicate value;
	public Plan subPlan;
	
	public InPredicate(Predicate value, Plan subPlan) {
		this.value = value;
		this.subPlan = subPlan;
	}
	
	@Override
	public String toString() {
		String result = subPlan.toString() + "\n";
		result += "(" + value.toString() + " ) is in Plan %" + subPlan.getPlanID();
		return result;
	}

	@Override
	public Data calc(Record record) {
		// TODO Auto-generated method stub
		return null;
	}
}
