package fatworm.engine.predicate;

import fatworm.engine.symbol.Symbol;

public class BooleanCompPredicate extends Predicate {
	
	public Predicate lhs, rhs;
	public int oper;
	
	public BooleanCompPredicate(Predicate lhs, Predicate rhs, int oper) {
		this.lhs = lhs;
		this.rhs = rhs;
		this.oper = oper;
	}
	
	@Override
	public String toString() {
		String result = "( " + lhs.toString() + " )";
		if (oper == Symbol.LESS) {
			result += " < ";
		} else if (oper == Symbol.GTR) {
			result += " > ";
		} else if (oper == Symbol.EQ) {
			result += " = ";
		} else if (oper == Symbol.LESS_EQ) {
			result += " <= ";
		} else if (oper == Symbol.GTR_EQ) {
			result += " >= ";
		} else if (oper == Symbol.NEQ) {
			result += " <> ";
		}
		result += "( " + rhs.toString() + " )";
		return result;
	}
	
}
