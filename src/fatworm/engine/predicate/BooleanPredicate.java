package fatworm.engine.predicate;

import fatworm.engine.symbol.Symbol;
import fatworm.indexing.data.BooleanData;
import fatworm.indexing.data.BooleanType;
import fatworm.indexing.data.Data;
import fatworm.indexing.data.DataType;
import fatworm.indexing.table.Record;

public class BooleanPredicate extends Predicate {
	
	public Predicate lhs, rhs;
	public int oper;
	public DataType type;
	
	public BooleanPredicate(Predicate lhs, Predicate rhs, int oper) {
		this.lhs = lhs;
		this.rhs = rhs;
		this.oper = oper;
		this.type = new BooleanType();
	}
	
	@Override
	public String toString() {
		return "(" + lhs.toString() + " )" + (oper == Symbol.OR ? " or " : " and ") + "( " + rhs.toString() + " )";
	}

	@Override
	public Data calc(Record record) {
		Data d1 = lhs.calc(record);
		Data d2 = rhs.calc(record);
		if (!(d1 instanceof BooleanData) || !(d2 instanceof BooleanData)) {
			try {
				throw new Exception("BooleanPredicate Error");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		BooleanData left = (BooleanData)d1;
		BooleanData right = (BooleanData)d2;
		BooleanData result = null;
		if (oper == Symbol.AND) {
			result = left.and(right);
		} else if (oper == Symbol.OR) {
			result = left.or(right);
		}
		return result;
	}

	@Override
	public DataType getType() {
		return type;
	}
}
