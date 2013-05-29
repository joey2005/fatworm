package fatworm.engine.predicate;

import fatworm.engine.plan.Plan;
import fatworm.indexing.data.BooleanData;
import fatworm.indexing.data.BooleanType;
import fatworm.indexing.data.Data;
import fatworm.indexing.data.DataType;
import fatworm.indexing.scan.Scan;
import fatworm.indexing.table.Record;

public class InPredicate extends Predicate {
	
	public Predicate value;
	public Plan subPlan;
	
	private DataType type;
	
	public InPredicate(Predicate value, Plan subPlan) {
		this.value = value;
		this.subPlan = subPlan;
		type = new BooleanType();
	}
	
	@Override
	public String toString() {
		String result = subPlan.toString() + "\n";
		result += "(" + value.toString() + " ) is in Plan %" + subPlan.getPlanID();
		return result;
	}

	@Override
	public Data calc(Record record) {
		if (subPlan.getSchema().getColumnCount() != 1) {
			try {
				throw new Exception("different type in predicate");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		Data result = value.calc(record);
		if (result.isNull()) {
			return BooleanData.NULL;
		}
		Scan s = subPlan.createScan();
		for (s.beforeFirst(); s.hasNext(); ) {
			Record now = s.next();
			Data data = now.getFromColumn(0);
			if (result.compareTo(data) == 0) {
				return BooleanData.TRUE;
			}
		}
		return BooleanData.FALSE;
	}

	@Override
	public DataType getType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean existsFunction() {
		return value.existsFunction();
	}
}
