package fatworm.engine.predicate;

import fatworm.engine.symbol.Symbol;
import fatworm.indexing.data.Data;
import fatworm.indexing.data.DataType;
import fatworm.indexing.data.NumberData;
import fatworm.indexing.data.NumberType;
import fatworm.indexing.table.Record;

public class NumberCalcPredicate extends Predicate {

	public Predicate lhs, rhs;
	public int oper;
	
	private NumberType type;
	
	public NumberCalcPredicate(Predicate lhs, Predicate rhs, int oper, NumberType type) {
		this.lhs = lhs;
		this.rhs = rhs;
		this.oper = oper;
		this.type = type;
	}
	
	@Override
	public String toString() {
		String result = "( " + lhs.toString() + " )";
		if (oper == Symbol.PLUS) {
			result += " + ";
		} else if (oper == Symbol.MINUS) {
			result += " - ";
		} else if (oper == Symbol.MUL) {
			result += " * ";
		} else if (oper == Symbol.DIV) {
			result += " / ";
		} else if (oper == Symbol.MOD) {
			result += " % ";
		}
		result += "( " + rhs.toString() + " )";
		return result;
	}

	@Override
	public Data calc(Record record) {
		Data d1 = lhs.calc(record);
		Data d2 = rhs.calc(record);
		if (d1.isNull()) return d1;
		if (d2.isNull()) {
			return d2;
		}
		if (!(d1 instanceof NumberData) || !(d2 instanceof NumberData)) {
			try {
				throw new Exception("NumberCalcPredicate error");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		NumberData left = (NumberData) d1;
		NumberData right = (NumberData) d2;
		if (oper == Symbol.PLUS) {
			return NumberData.add(left, right, type);
		} else if (oper == Symbol.MINUS) {
			return NumberData.subtract(left, right, type);
		} else if (oper == Symbol.MUL) {
			return NumberData.multiply(left, right, type);
		} else if (oper == Symbol.DIV) {
			return NumberData.divide(left, right, type);
		} else if (oper == Symbol.MOD) {
			return NumberData.mod(left, right, type);
		}
		return null;
	}

	public NumberType getType() {
		return type;
	}

	@Override
	public boolean existsFunction() {
		return lhs.existsFunction() || rhs.existsFunction();
	}
}
