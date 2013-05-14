package fatworm.engine.predicate;

import fatworm.engine.symbol.Symbol;
import fatworm.indexing.data.Data;
import fatworm.indexing.table.Record;

public class NumberCalcPredicate extends Predicate {

	public Predicate lhs, rhs;
	public int oper;
	
	public NumberCalcPredicate(Predicate lhs, Predicate rhs, int oper) {
		this.lhs = lhs;
		this.rhs = rhs;
		this.oper = oper;
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
		// TODO Auto-generated method stub
		return null;
	}
}
