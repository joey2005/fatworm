package fatworm.engine.predicate;

import fatworm.engine.plan.Plan;
import fatworm.engine.symbol.Symbol;
import fatworm.indexing.data.BooleanData;
import fatworm.indexing.data.BooleanType;
import fatworm.indexing.data.Data;
import fatworm.indexing.data.DataType;
import fatworm.indexing.scan.Scan;
import fatworm.indexing.table.Record;

public class AnyPredicate extends Predicate {

	public Predicate value;
	public int oper;
	public Plan subPlan;
	
	private DataType type;
	
	public AnyPredicate(Predicate value, String oper, Plan subPlan) {
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
		type = new BooleanType();
	}
	
	public AnyPredicate(Predicate value, int oper, Plan subPlan) {
		this.value = value;
		this.oper = oper;
		this.subPlan = subPlan;
		type = new BooleanType();
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
		result += "any of ( " + subPlan.toString() + " )";
		return result;
	}

	@Override
	public Data calc(Record record) {
		if (subPlan.getSchema().getColumnCount() != 1) {
			try {
				throw new Exception("different type in predicate");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		Data result = value.calc(record);
		Scan s = subPlan.createScan();
		for (s.beforeFirst(); s.hasNext(); ) {
			Record now = s.next();
			Data data = now.getFromColumn(0);
			BooleanCompPredicate test = new BooleanCompPredicate(
					new ConstantPredicate(result),
					new ConstantPredicate(data),
					oper);
			if (test.calc(null).equals(BooleanData.TRUE)) {
				return BooleanData.TRUE;
			}
		}
		return BooleanData.FALSE;
	}

	@Override
	public DataType getType() {
		return type;
	}

	@Override
	public boolean existsFunction() {
		return value.existsFunction();
	}
}
