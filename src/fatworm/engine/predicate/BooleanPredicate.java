package fatworm.engine.predicate;

import fatworm.engine.symbol.Symbol;
import fatworm.indexing.data.Data;
import fatworm.indexing.table.Record;

public class BooleanPredicate extends Predicate {
	
	public Predicate lhs, rhs;
	public int oper;
	
	public BooleanPredicate(Predicate lhs, Predicate rhs, int oper) {
		this.lhs = lhs;
		this.rhs = rhs;
		this.oper = oper;
	}
	
	@Override
	public String toString() {
		return "(" + lhs.toString() + " )" + (oper == Symbol.OR ? " or " : " and ") + "( " + rhs.toString() + " )";
	}

	@Override
	public Data calc(Record record) {
		// TODO Auto-generated method stub
		return null;
	}
}
