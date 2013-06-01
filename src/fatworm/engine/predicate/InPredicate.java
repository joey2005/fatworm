package fatworm.engine.predicate;

import fatworm.engine.plan.Plan;
import fatworm.indexing.data.BooleanData;
import fatworm.indexing.data.BooleanType;
import fatworm.indexing.data.Data;
import fatworm.indexing.data.DataType;
import fatworm.indexing.scan.Scan;
import fatworm.indexing.table.Record;
import fatworm.util.Fatworm;

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
		Fatworm.paths.add(record);
		Scan s = subPlan.createScan();
		for (s.beforeFirst(); s.hasNext(); ) {
			Record now = s.next();
			Data data = now.getFromColumn(0);
			if (result.compareTo(data) == 0) {
				Fatworm.paths.remove(Fatworm.paths.size() - 1);
				return BooleanData.TRUE;
			}
		}
		Fatworm.paths.remove(Fatworm.paths.size() - 1);
		return BooleanData.FALSE;
	}

	@Override
	public DataType getType() {
		return type;
	}

	@Override
	public boolean existsFunction() {
		return value.existsFunction();
	}
}
