package fatworm.indexing.data;

import java.math.BigDecimal;

import fatworm.util.Lib;

public class IntegerData extends NumberData {

	private Integer i;
	private IntegerType type;
	
	public IntegerData(Integer i, IntegerType type) {
		this.i = i;
		this.type = type;
	}

	public IntegerData(String s, IntegerType type) {
		if (s == null || s.equals("null")) {
			this.i = null;
		} else {
			this.i = Integer.parseInt(s);
		}
		this.type = type;
	}

	@Override
	public boolean isNull() {
		return i == null;
	}

	public int compareTo(Data o) {
		if (o instanceof NumberData) {
			if (isNull() && o.isNull()) {
				return 0;
			}
			if (isNull() || o.isNull()) {
				return 0x7f7f7f7f;
			}
			if (o instanceof IntegerData) {
				IntegerData data = (IntegerData) o;
				return i.compareTo(data.i);
			}
			if (o instanceof FloatData) {
				FloatData data = (FloatData) o;
				return -data.compareTo(this);
			}
			if (o instanceof DecimalData) {
				DecimalData data = (DecimalData) o;
				return -data.compareTo(this);
			}
		}	
		return 0x0fffffff;
	}

	@Override
	public String toString() {
		if (i == null) {
			return null;
		}
		return i.toString();
	}

	@Override
	public DataType getType() {
		return type;
	}

	@Override
	public BigDecimal DecimalValue() {
		if (i == null) {
			return null;
		}
		return BigDecimal.valueOf(i);
	}

	@Override
	public Float FloatValue() {
		if (i == null) {
			return null;
		}
		return Float.valueOf(i);
	}

	@Override
	public Integer IntegerValue() {
		if (i == null) {
			return null;
		}
		return i;
	}

	@Override
	public int sign() {
		return i == 0 ? 0 : i > 0 ? 1 : -1;
	}
	
	public Integer getValue() {
		return i;
	}

	@Override
	public boolean equals(Object obj) {
		return this.compareTo((Data)obj) == 0;
	}

	@Override
	public String storageValue() {
		if (i == null) {
			return "null";
		}
		return i.toString();
	}

}
