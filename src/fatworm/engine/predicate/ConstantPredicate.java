package fatworm.engine.predicate;

import fatworm.indexing.data.Data;
import fatworm.indexing.data.DataType;
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
		return data;
	}

	@Override
	public DataType getType() {
		return data.getType();
	}

	@Override
	public boolean existsFunction() {
		return false;
	}
}
