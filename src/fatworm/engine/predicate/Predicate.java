package fatworm.engine.predicate;

import fatworm.indexing.data.*;
import fatworm.indexing.table.Record;

public abstract class Predicate {
	
	@Override
	public abstract String toString();

	public BooleanData test(Record record) throws Exception {
		Data result = calc(record);
		if (!(result instanceof BooleanData)) {
			throw new Exception("Not type of boolean");
		}
		return (BooleanData) result;
	}
	
	public abstract Data calc(Record record);
	
	public abstract DataType getType();
}
