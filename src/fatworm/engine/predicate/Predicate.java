package fatworm.engine.predicate;

import fatworm.indexing.data.*;
import fatworm.indexing.table.Record;

public abstract class Predicate {

	public BooleanData test(Record record) throws Exception {
		Data result = calc(record);
		if (!(result instanceof BooleanData)) {
			throw new Exception("Not type of boolean");
		}
		return (BooleanData) result;
	}
	
	public abstract Data calc(Record record);
}
