package fatworm.indexing.data;

public class VarcharData extends StringData {

	private String c;
	private VarcharType type;
		
	public VarcharData(String c, VarcharType type) {
		if (c == null)
			this.c = null;
		else {
			this.c = c;
		}
		this.type = type;
	}

	@Override
	public DataType getType() {
		return type;
	}

	public String getValue() {
		return c;
	}

	public VarcharData append(VarcharData other, VarcharType type) {
		return new VarcharData(c + other.c, type);
	}

	@Override
	public String toString() {
		return c;
	}

	@Override
	public boolean equals(Object obj) {
		return this.compareTo((Data)obj) == 0;
	}

	@Override
	public String storageValue() {
		if (c == null) {
			return type.getDefaultValue().toString();
		}
		if (c.length() > type.storageRequired()) {
			return c.substring(0, type.storageRequired());
		}
		String result = c;
		while (result.length() < type.storageRequired()) {
			result += '\0';
		}
		return result;
	}
	
}
