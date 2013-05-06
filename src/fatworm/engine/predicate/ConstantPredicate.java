package fatworm.engine.predicate;

import fatworm.indexing.data.Data;

public class ConstantPredicate extends Predicate {
	
	public Data data;
	
	public ConstantPredicate(Data data) {
		this.data = data;
	}
	
	@Override
	public String toString() {
		return data.toString();
	}
}
