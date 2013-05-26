package fatworm.indexing.data;

public class BooleanData extends Data {
	
	private Boolean b;
	private BooleanType type;

	public BooleanData(Boolean b, BooleanType type) {
		this.b = b;
		this.type = type;
	}
	
	public BooleanData(String s, BooleanType type) {
		if (s == null || s.equals("null")) {
			b = null;
		} else {
			if (s.length() > 0) {
				b = Boolean.valueOf((s.charAt(0) != 0));
			} else {
				b = null;
			}
		}
		this.type = type;
	}

	@Override
	public int compareTo(Data args0) {
		if (args0 instanceof BooleanData) {
			BooleanData o = (BooleanData)args0;
			if (this.isNull()) {
				return o.isNull() ? 0 : -1;
			}
			if (o.isNull()) {
				return 1;
			}
			return b.compareTo(o.b);
		}
		return 0x0fffffff;
	}

	@Override
	public boolean isNull() {
		return b == null;
	}

	@Override
	public DataType getType() {
		return type;
	}

	@Override
	public String toString() {
		if (b == null) {
			return null;
		}
		return b.toString();
	}

	@Override
	public Object getValue() {
		return this.b;
	}

	public BooleanData and(BooleanData o) {
		if (this.isNull() || o.isNull()) {
			return BooleanData.NULL;
		}
		return new BooleanData(this.b && o.b, this.type);
	}
	
	public BooleanData or(BooleanData o) {
		if (this.isNull() || o.isNull()) {
			return BooleanData.NULL;
		}
		return new BooleanData(this.b || o.b, this.type);
	}	

	public static final BooleanData TRUE = new BooleanData(true, new BooleanType());
	public static final BooleanData FALSE = new BooleanData(false, new BooleanType());
	public static final BooleanData NULL = new BooleanData((Boolean)null, new BooleanType());

	@Override
	public boolean equals(Object obj) {
		return this.compareTo((Data)obj) == 0;
	}

	@Override
	public String storageValue() {
		if (b == null) {
			return type.getDefaultValue().storageValue();
		}
		byte[] buf = new byte[1];
		if (b) {
			buf[0] = 1;
		} else {
			buf[0] = 0;
		}
		return new String(buf);
	}
}
