package fatworm.engine.predicate;

import fatworm.indexing.data.Data;
import fatworm.indexing.data.DataType;
import fatworm.indexing.table.Record;

public class VariablePredicate extends Predicate {
	
	public String variableName;

	public VariablePredicate(String variableName) {
		this.variableName = variableName;
	}
	
	@Override
	public String toString() {
		return variableName;
	}

	@Override
	public Data calc(Record record) {
		// TODO Auto-generated method stub
		return null;
	}
}
