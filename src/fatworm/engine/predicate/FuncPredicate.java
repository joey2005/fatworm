package fatworm.engine.predicate;

import java.util.List;

import fatworm.engine.symbol.Symbol;
import fatworm.indexing.data.Data;
import fatworm.indexing.data.DataType;
import fatworm.indexing.data.DecimalData;
import fatworm.indexing.data.DecimalType;
import fatworm.indexing.data.FloatData;
import fatworm.indexing.data.FloatType;
import fatworm.indexing.data.IntegerData;
import fatworm.indexing.data.IntegerType;
import fatworm.indexing.data.NumberData;
import fatworm.indexing.data.NumberType;
import fatworm.indexing.scan.Scan;
import fatworm.indexing.table.Record;

public class FuncPredicate extends Predicate {
	
	public int func;
	public VariablePredicate colName;
	public DataType type;
	public Data result;
	
	public FuncPredicate(int func, VariablePredicate colName) {
		this.func = func;
		this.colName = colName;
		try {
			if (func == Symbol.COUNT) {
				type = new IntegerType();
			} else if (func == Symbol.AVG) {
				if (!(colName.getType() instanceof NumberType)) {
					throw new Exception("FuncPredicate error: avg");
				}
				type = new DecimalType(20, 10);
			} else {
				type = colName.getType();
				if (!(type instanceof NumberType) && func == Symbol.SUM) {
					throw new Exception("FuncPredicate error: sum");
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
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
			DecimalType dtype = (DecimalType)type;
			result = dtype.valueOf("0");
			int count = 0;
			for (Record record : sublists) {
				NumberData now = (NumberData)record.getFromVariableName(colName.toString());
				if (now.getValue() != null) {
					result = NumberData.add((NumberData)result, now, dtype);
					count++;
				}
			}
			if (count > 0) {
				result = NumberData.divide((NumberData)result, new IntegerData(count, new IntegerType()), dtype);
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
			for (Record record : sublists) {
				Data now = record.getFromVariableName(colName.toString());
				if (now.getValue() != null) {
					if (result == null) {
						try {
							result = type.valueOf(now);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					} else if (type instanceof IntegerType) {
						result = NumberData.add((IntegerData)result, (IntegerData)now, (IntegerType)type);
					} else if (type instanceof FloatType) {
						result = NumberData.add((FloatData)result, (FloatData)now, (FloatType)type);
					} else if (type instanceof DecimalType) {
						result = NumberData.add((DecimalData)result, (DecimalData)now, (DecimalType)type);
					}
				}
			}
		}
	}

	@Override
	public Data calc(Record record) {
		return result;
	}

	@Override
	public DataType getType() {
		return type;
	}
}
