package fatworm.indexing.data;

import java.math.BigDecimal;
import java.math.BigInteger;


public class DecimalType extends NumberType {

	private int precision;
	private int scale;
	private int storageRequired;
	private DecimalData defaultValue = null;
	
	public DecimalType(int precision, int scale) {
		this.precision = precision;
		this.scale = scale;
		this.storageRequired = precision + scale + 1;
	}

	@Override
	public Data valueOf(Data data) throws Exception {
		if (data.getType() instanceof NumberType) {
			NumberData num = (NumberData)data;
			Data result = new DecimalData(num.DecimalValue(), this);
			return result;
		}
		throw new Exception("Data Format Wrong");
	}
	
	@Override
	public Data getDefaultValue() {
		if (defaultValue == null) {
			defaultValue = new DecimalData(BigDecimal.ZERO, this);
		}
		return defaultValue;
	}
	
	@Override
	public int storageRequired() {
		return storageRequired;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof DecimalType) {
			DecimalType decimalType = (DecimalType)o;
			return precision == decimalType.precision && scale == decimalType.scale;
		}
		return false;
	}

	@Override
	public String toString() {
		return "Decimal(" + precision + ", " + scale + ")";
	}

	@Override
	public Data valueOf(String value) {
		return new DecimalData(value, this, 0);
	}

	public int getPrecision() {
		return precision;
	}

	public int getScale() {
		return scale;
	}
	
	@Override
	public int encode() {
		return setSecondArg(setFirstArg(DECIMAL, precision), scale);
	}
	
}
