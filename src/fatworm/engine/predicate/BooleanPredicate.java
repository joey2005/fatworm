package fatworm.engine.predicate;

import fatworm.engine.symbol.Symbol;

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
}
