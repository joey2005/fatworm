package fatworm.engine.predicate;

import fatworm.indexing.data.Data;
import fatworm.indexing.table.Record;

public class ConstantPredicate extends Predicate {
	
	public Data data;
	
	public ConstantPredicate(Data data) {
		this.data = data;
	}
	
	@Override
	public String toString() {
		return data.toString();
	}

	@Override
	public Data calc(Record record) {
		// TODO Auto-generated method stub
		return null;
	}
}
