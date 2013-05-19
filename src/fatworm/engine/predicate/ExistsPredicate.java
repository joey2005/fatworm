package fatworm.engine.predicate;

import fatworm.engine.plan.Plan;
import fatworm.indexing.data.BooleanData;
import fatworm.indexing.data.BooleanType;
import fatworm.indexing.data.Data;
import fatworm.indexing.data.DataType;
import fatworm.indexing.scan.Scan;
import fatworm.indexing.table.Record;

public class ExistsPredicate extends Predicate {
	
	public boolean neg;
	public Plan subPlan;
	public DataType type;

	public ExistsPredicate(boolean neg, Plan subPlan) {
		this.neg = neg;
		this.subPlan = subPlan;
		type = new BooleanType();
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
		Scan s = subPlan.createScan();
		s.beforeFirst();
		boolean exists = s.hasNext();
		return new BooleanData(exists, new BooleanType());
	}

	@Override
	public DataType getType() {
		// TODO Auto-generated method stub
		return null;
	}
}
