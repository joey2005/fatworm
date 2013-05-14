package fatworm.engine.predicate;

import fatworm.engine.symbol.Symbol;
import fatworm.indexing.data.Data;
import fatworm.indexing.table.Record;

public class FuncPredicate extends Predicate {
	
	public int func;
	public String colName;
	
	public FuncPredicate(int func, String colName) {
		this.func = func;
		this.colName = colName;
	}
	
	@Override
	public String toString() {
		String result = "";
		if (func == Symbol.AVG) {
			result = "AVG";
		} else if (func == Symbol.MAX) {
			result = "MAX";
		} else if (func == Symbol.MIN) {
			result = "MIN";
		} else if (func == Symbol.COUNT) {
			result = "COUNT";
		} else if (func == Symbol.SUM) {
			result = "SUM";
		}
		result += "( " + colName + " )";
		return result;
	}

	@Override
	public Data calc(Record record) {
		// TODO Auto-generated method stub
		return null;
	}
}
