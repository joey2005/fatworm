package fatworm.engine.predicate;

import fatworm.indexing.data.DataType;

public class VariablePredicate extends Predicate {
	
	public String variableName;
	public DataType dataType = null;

	public VariablePredicate(String variableName) {
		this.variableName = variableName;
	}
	
	@Override
	public String toString() {
		return variableName;
	}
}
