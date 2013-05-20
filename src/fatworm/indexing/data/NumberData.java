package fatworm.indexing.data;

import java.math.BigDecimal;

public abstract class NumberData extends Data {

	public abstract int sign();
	
	public abstract Integer IntegerValue();
	
	public abstract Float FloatValue();
	
	public abstract BigDecimal DecimalValue();

	public static NumberData add(NumberData left, NumberData right, NumberType type) {
		try {
			if (left.isNull()) {
				return (NumberData)type.valueOf(right);
			}
			if (right.isNull()) {
				return (NumberData)type.valueOf(left);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		if (type instanceof IntegerType) {
			return new IntegerData(left.IntegerValue() + right.IntegerValue(), 
					(IntegerType)type);
		}
		if (type instanceof FloatType) {
			return new FloatData(left.FloatValue() + right.FloatValue(),
					(FloatType)type); 
		}
		return new DecimalData(left.DecimalValue().add(right.DecimalValue()),
				(DecimalType)type);
	}
	
	public static NumberData subtract(NumberData left, NumberData right, NumberType type) {
		try {
			if (left.isNull()) {
				return (NumberData)type.valueOf(right);
			}
			if (right.isNull()) {
				return (NumberData)type.valueOf(left);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		if (type instanceof IntegerType) {
			return new IntegerData(left.IntegerValue() - right.IntegerValue(), 
					(IntegerType)type);
		}
		if (type instanceof FloatType) {
			return new FloatData(left.FloatValue() - right.FloatValue(),
					(FloatType)type); 
		}
		return new DecimalData(left.DecimalValue().subtract(right.DecimalValue()),
				(DecimalType)type);
	}
	
	public static NumberData multiply(NumberData left, NumberData right, NumberType type) {
		try {
			if (left.isNull() || right.isNull()) {
				return (NumberData)type.valueOf((String)null);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		if (type instanceof IntegerType) {
			return new IntegerData(left.IntegerValue() * right.IntegerValue(), 
					(IntegerType)type);
		}
		if (type instanceof FloatType) {
			return new FloatData(left.FloatValue() * right.FloatValue(),
					(FloatType)type); 
		}
		return new DecimalData(left.DecimalValue().multiply(right.DecimalValue()),
				(DecimalType)type);
	}
	
	public static NumberData divide(NumberData left, NumberData right, NumberType type) {
		try {
			if (left.isNull() || right.isNull()) {
				return (NumberData)type.valueOf((String)null);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		if (type instanceof IntegerType) {
			return new IntegerData(left.IntegerValue() / right.IntegerValue(), 
					(IntegerType)type);
		}
		if (type instanceof FloatType) {
			return new FloatData(left.FloatValue() / right.FloatValue(),
					(FloatType)type); 
		}
		return new DecimalData(left.DecimalValue().divide(right.DecimalValue()),
				(DecimalType)type);
	}
	
	public static NumberData mod(NumberData left, NumberData right, NumberType type) {
		try {
			if (left.isNull() || right.isNull()) {
				return (NumberData)type.valueOf((String)null);
			}
			if (!(left.getType() instanceof IntegerType) || 
					!(right.getType() instanceof IntegerType)) {
				throw new Exception("NumberData error: mod not integer");
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return new IntegerData(left.IntegerValue() % right.IntegerValue(), 
				(IntegerType)type);
	}
}
