package fatworm.engine.predicate;

import java.util.*;

import fatworm.engine.symbol.Symbol;
import fatworm.indexing.data.*;
import fatworm.indexing.scan.Scan;
import fatworm.indexing.table.Record;

public class FuncPredicate extends Predicate {
	
	public int func;
	public VariablePredicate colName;
	public Data result;
	
	private DataType type;
	private NumberType restype;
	
	public FuncPredicate(int func, VariablePredicate colName) {
		this.func = func;
		this.colName = colName;
		try {
			restype = null;
			if (func == Symbol.COUNT) {
				type = new IntegerType();
			} else if (func == Symbol.AVG || func == Symbol.SUM) {
				type = colName.getType();
				if (!(type instanceof NumberType)) {
					throw new RuntimeException("cannot resolve a type");
				}
				if (type instanceof IntegerType) {
					restype = new FloatType();
				} else if (type instanceof FloatType) {
					restype = (FloatType) type;
				} else if (type instanceof DecimalType) {
					restype = (DecimalType) type;
				}
			} else {
				type = colName.getType();
			}
		} catch (Exception ex) {
			throw new RuntimeException("FuncPredicate construct error");
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
	
	public void prepare(List<Record> sublists) {
		result = null;
		
		if (func == Symbol.AVG) {
			result = restype.valueOf("0");
			int count = 0;
			for (Record record : sublists) {
				NumberData now = (NumberData)record.getFromVariableName(colName.toString());
				if (now.getValue() != null) {
					result = NumberData.add((NumberData)result, now, restype);
					count++;
				}
			}
			if (count > 0) {
				result = NumberData.divide((NumberData)result, new IntegerData(count, new IntegerType()), restype);
			} else {
				result = null;
			}
		} else if (func == Symbol.MAX) {
			for (Record record : sublists) {
				Data now = record.getFromVariableName(colName.toString());
				if (now.getValue() != null) {
					if (result == null || now.compareTo(result) > 0) {
						result = now;
					}
				}
			}
		} else if (func == Symbol.MIN) {
			for (Record record : sublists) {
				Data now = record.getFromVariableName(colName.toString());
				if (now.getValue() != null) {
					if (result == null || now.compareTo(result) < 0) {
						result = now;
					}
				}
			}			
		} else if (func == Symbol.COUNT) {
			int count = 0;
			for (Record record : sublists) {
				Data now = record.getFromVariableName(colName.toString());
				if (now.getValue() != null) {
					count++;
				}
			}
			result = new IntegerData(count, new IntegerType());
		} else if (func == Symbol.SUM) {
			result = restype.valueOf("0");
			int count = 0;
			for (Record record : sublists) {
				Data now = record.getFromVariableName(colName.toString());
				if (now.getValue() != null) {
					if (type instanceof IntegerType) {
						result = NumberData.add((FloatData)result, (IntegerData)now, restype);
					} else if (type instanceof FloatType) {
						result = NumberData.add((FloatData)result, (FloatData)now, restype);
					} else if (type instanceof DecimalType) {
						result = NumberData.add((DecimalData)result, (DecimalData)now, restype);
					}
					count++;
				}
			}
			if (count == 0) {
				result = null;
			}
		}
	}

	@Override
	public Data calc(Record record) {
		return result;
	}

	@Override
	public DataType getType() {
		if (restype != null) {
			return restype;
		}
		return type;
	}

	@Override
	public boolean existsFunction() {
		return true;
	}
}
