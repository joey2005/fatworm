package fatworm.engine.predicate;

import fatworm.engine.symbol.Symbol;
import fatworm.indexing.data.BooleanData;
import fatworm.indexing.data.BooleanType;
import fatworm.indexing.data.Data;
import fatworm.indexing.data.DataType;
import fatworm.indexing.data.NumberData;
import fatworm.indexing.data.StringData;
import fatworm.indexing.table.Record;

public class BooleanCompPredicate extends Predicate {
	
	public Predicate lhs, rhs;
	public int oper;
	public DataType type;
	
	public BooleanCompPredicate(Predicate lhs, Predicate rhs, int oper) {
		this.lhs = lhs;
		this.rhs = rhs;
		this.oper = oper;
		this.type = new BooleanType();
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

	@Override
	public Data calc(Record record) {
		Data d1 = lhs.calc(record);
		Data d2 = rhs.calc(record);
		boolean n1 = (d1 instanceof NumberData);
		boolean n2 = (d2 instanceof NumberData);
		if (n1 != n2) {
			try {
				throw new Exception("BooleanCompPredicate Error");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		int compKey = -1;
		if (!n1) {
			if ((d1 instanceof StringData) || (d2 instanceof StringData)) {
				compKey = d1.toString().compareTo(d2.toString());
			} else {
				java.sql.Date left = (java.sql.Date)d1.getValue();
				java.sql.Date right = (java.sql.Date)d2.getValue();
				compKey = left.compareTo(right);
			}
		} else {
			compKey = ((NumberData)d1).compareTo((NumberData)d2);
		}
		if (oper == Symbol.GTR) {
			return new BooleanData(compKey > 0, new BooleanType());
		} else if (oper == Symbol.GTR_EQ) {
			return new BooleanData(compKey >= 0, new BooleanType());
		} else if (oper == Symbol.LESS) {
			return new BooleanData(compKey < 0, new BooleanType());
		} else if (oper == Symbol.LESS_EQ) {
			return new BooleanData(compKey <= 0, new BooleanType());
		} else if (oper == Symbol.EQ) {
			return new BooleanData(compKey == 0, new BooleanType());
		} else if (oper == Symbol.NEQ) {
			return new BooleanData(compKey != 0, new BooleanType());
		}
		return null;
	}

	@Override
	public DataType getType() {
		return type;
	}
	
}
