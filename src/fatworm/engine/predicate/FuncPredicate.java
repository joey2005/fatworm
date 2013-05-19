package fatworm.engine.predicate;

import fatworm.engine.symbol.Symbol;
import fatworm.indexing.data.Data;
import fatworm.indexing.data.DataType;
import fatworm.indexing.data.DecimalType;
import fatworm.indexing.data.IntegerType;
import fatworm.indexing.table.Record;

public class FuncPredicate extends Predicate {
	
	public int func;
	public VariablePredicate colName;
	public DataType type;
	
	public FuncPredicate(int func, VariablePredicate colName) {
		this.func = func;
		this.colName = colName;
		if (func == Symbol.COUNT) {
			type = new IntegerType();
		} else if (func == Symbol.AVG) {
			type = new DecimalType(20, 10);
		} else {
			type = colName.getType();
		}
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
		result += "( " + colName.toString() + " )";
		return result;
	}

	@Override
	public Data calc(Record record) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataType getType() {
		return type;
	}
}
