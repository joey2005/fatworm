package fatworm.indexing.data;

public class CharData extends StringData {

	public CharData(String c, CharType type) {
		if (c == null) {
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

	private String c;
	private CharType type;
}
