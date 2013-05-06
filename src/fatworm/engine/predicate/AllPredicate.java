package fatworm.engine.predicate;

import fatworm.engine.plan.Plan;
import fatworm.engine.symbol.Symbol;

public class AllPredicate extends Predicate {

	public Predicate value;
	public int oper;
	public Plan subPlan;
	
	public AllPredicate(Predicate value, String oper, Plan subPlan) {
		this.value = value;
		if (oper.startsWith("<=")) {
			this.oper = Symbol.LESS_EQ;
		} else if (oper.startsWith(">=")) {
			this.oper = Symbol.GTR_EQ;
		} else if (oper.startsWith("<>")) {
			this.oper = Symbol.NEQ;
		} else if (oper.startsWith("<")) {
			this.oper = Symbol.LESS;
		} else if (oper.startsWith(">")) {
			this.oper = Symbol.GTR;
		} else if (oper.startsWith("=")) {
			this.oper = Symbol.EQ;
		}
		this.subPlan = subPlan;
	}
	
	@Override
	public String toString() {
		String result = "( " + value.toString() + " )";
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
		result += "all of ( " + subPlan.toString() + " )";
		return result;
	}
}
