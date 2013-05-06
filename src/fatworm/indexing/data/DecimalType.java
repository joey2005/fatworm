package fatworm.indexing.data;

import java.math.BigDecimal;
import java.math.BigInteger;


public class DecimalType extends NumberType {

	public DecimalType(int precision, int scale) {
		this.precision = precision;
		this.scale = scale;
		this.storageRequired = getStoreSize(precision);
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

	protected static int getStoreSize(int k) {
		if (k == 0) {
			return 0;
		}
		int result = 0;
		BigInteger b = BigInteger.ONE;
		BigInteger two = b.add(BigInteger.ONE);
		for (int i = 0; i < k; i++) {
			b = b.multiply(BigInteger.valueOf(10));
		}
		while (b.compareTo(BigInteger.ONE) >= 0) {
			b = b.divide(two);
			result++;
		}
		return (result - 1) / 8 + 1;
	}

	private int precision, scale, storageRequired;
	private DecimalData defaultValue = null;
}
