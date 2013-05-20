package fatworm.indexing.data;

import java.math.BigDecimal;
import java.math.BigInteger;

public class DecimalData extends NumberData {

	private BigDecimal d;
	private DecimalType type;

	public DecimalData(String s, DecimalType type) {
		this.type = type;
		if (s != null) {
			this.d = new BigDecimal(s);
		} else {
			this.d = null;
		}
	}

	public DecimalData(String s, DecimalType type, int added) {
		this.type = type;
		if (s == null || s.equals("null")) {
			this.d = null;
		} else {
			BigInteger b = new BigInteger(s);
			this.d = new BigDecimal(b);
			this.d = this.d.scaleByPowerOfTen(-type.getScale());
		}
	}

	protected DecimalData(BigDecimal d, DecimalType type) {
		this.d = d;
		this.type = type;
	}

	public int compareTo(Data o) {
		if (o instanceof NumberData) {
			if (this.isNull()) {
				return o.isNull() ? 0 : -1;
			}
			if (o.isNull()) {
				return 1;
			}
			return this.d.compareTo(((NumberData) o).DecimalValue());
		}
		return 0x0fffffff;
	}

	@Override
	public int sign() {
		return d.signum();
	}

	@Override
	public DataType getType() {
		return type;
	}

	@Override
	public boolean isNull() {
		return d == null;
	}

	@Override
	public String toString() {
		if (d == null) {
			return null;
		}
		return d.toPlainString();
	}

	@Override
	public BigDecimal DecimalValue() {
		if (d == null) {
			return null;
		}
		return d;
	}

	@Override
	public Float FloatValue() {
		if (d == null) {
			return null;
		}
		return Float.valueOf(d.floatValue());
	}

	@Override
	public Integer IntegerValue() {
		if (d == null) {
			return null;
		}
		return Integer.valueOf(d.intValue());
	}

	public BigDecimal getValue() {
		return d;
	}

	@Override
	public boolean equals(Object obj) {
		return this.compareTo((Data)obj) == 0;
	}

}
