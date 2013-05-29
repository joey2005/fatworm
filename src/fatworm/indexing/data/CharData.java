package fatworm.indexing.data;

public class CharData extends StringData {

	private String c;
	private CharType type;
	
	public CharData(String c, CharType type) {
		if (c == null || c.equals("null")) {
			this.c = null;
		} else {
			this.c = c;
		}
		this.type = type;
	}

	@Override
	public DataType getType() {
		return type;
	}

	public String stringValue() {
		return c;
	}

	@Override
	public String toString() {
		return c;
	}

	public String getValue() {
		return c;
	}

	@Override
	public boolean equals(Object obj) {
		return this.compareTo((Data)obj) == 0;
	}

	@Override
	public String storageValue() {
		if (c == null) {
			return "null";
		}
		if (c.length() > type.storageRequired()) {
			return c.substring(0, type.storageRequired());
		}
		return c;
	}

}
