package fatworm.engine.predicate;

import fatworm.engine.plan.Plan;
import fatworm.indexing.data.Data;
import fatworm.indexing.table.Record;

public class SubQueryPredicate extends Predicate {
	
	public Plan subPlan;
	
	public SubQueryPredicate(Plan subPlan) {
		this.subPlan = subPlan;
	}
	
	@Override
	public String toString() {
		return subPlan.toString();
	}

	@Override
	public Data calc(Record record) {
		// TODO Auto-generated method stub
		return null;
	}
}
