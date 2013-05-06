package fatworm.indexing.data;

import java.math.BigDecimal;

public class FloatData extends NumberData {

	public FloatData(Float f, FloatType type) {
		this.f = f;
		this.type = type;
	}

	public FloatData(String s, FloatType type) {
		if (s != null) {
			this.f = Float.parseFloat(s);
		} else {
			this.f = null;
		}
		this.type = type;
	}

	public int compareTo(Data o) {
		if (o instanceof NumberData) {
			if (isNull()) {
				return o.isNull() ? 0 : -1;
			}
			if (o.isNull()) {
				return 1;
			}
			if (o instanceof FloatData) {
				FloatData data = (FloatData) o;
				return f.compareTo(data.f);
			}
			if (o instanceof IntegerData) {
				IntegerData data = (IntegerData) o;
				return Double.compare(f, data.IntegerValue());
			}
			if (o instanceof DecimalData) {
				DecimalData data = (DecimalData) o;
				return -data.compareTo(this);
			}
		}
		return 0x0fffffff;
	}

	@Override
	public boolean isNull() {
		return f == null;
	}

	@Override
	public String toString() {
		return f.toString();
	}

	@Override
	public DataType getType() {
		return type;
	}

	public int sign() {
		int t = f.compareTo(Float.valueOf(0));
		return t == 0 ? 0 : t < 0 ? -1 : 1;
	}

	@Override
	public BigDecimal DecimalValue() {
		if (f == null) {
			return null;
		}
		return BigDecimal.valueOf(f.floatValue());
	}

	@Override
	public Float FloatValue() {
		if (f == null) {
			return null;
		}
		return f;
	}

	@Override
	public Integer IntegerValue() {
		if (f == null) {
			return null;
		}
		return Integer.valueOf(f.intValue());
	}
	
	public Float getValue() {
		return f;
	}

	private Float f;
	private FloatType type;

}