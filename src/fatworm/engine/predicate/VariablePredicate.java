package fatworm.engine.predicate;

import fatworm.indexing.data.Data;
import fatworm.indexing.data.DataType;
import fatworm.indexing.table.Record;
import fatworm.util.Fatworm;

public class VariablePredicate extends Predicate {
	
	public String variableName;
	
	private DataType type;

	public VariablePredicate(String variableName, DataType type) {
		this.variableName = variableName;
		this.type = type;
	}
	
	@Override
	public String toString() {
		if (variableName.startsWith(".")) {
			return variableName.substring(1);
		}
		return variableName;
	}

	@Override
	public Data calc(Record record) {
		Data res = record.getFromVariableName(variableName);
		if (res == null) {
			for (int i = Fatworm.paths.size() - 1; i >= 0; --i) {
				res = Fatworm.paths.get(i).getFromVariableName(variableName);
				if (res != null) {
					break;
				}
			}
		}
		return res;
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
