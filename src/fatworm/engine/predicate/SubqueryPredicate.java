package fatworm.engine.predicate;

import fatworm.engine.plan.Plan;
import fatworm.indexing.data.Data;
import fatworm.indexing.data.DataType;
import fatworm.indexing.scan.Scan;
import fatworm.indexing.table.Record;
import fatworm.util.Fatworm;

public class SubQueryPredicate extends Predicate {
	
	private Plan subPlan;
	private DataType type;
	
	public SubQueryPredicate(Plan subPlan) {
		this.subPlan = subPlan;
		this.type = subPlan.getSchema().getFromColumn(0).getType();
	}
	
	@Override
	public String toString() {
		return subPlan.toString();
	}

	@Override
	public Data calc(Record record) {
		Fatworm.paths.add(record);
		Scan scan = subPlan.createScan();
		scan.beforeFirst();
		Record res = null;
		if (scan.hasNext()) {
			res = scan.next();
		}
		Fatworm.paths.remove(Fatworm.paths.size() - 1);
		return res.getFromColumn(0);
	}

	@Override
	public DataType getType() {
		return type;
	}

	@Override
	public boolean existsFunction() {
		return false;
	}
}
