package fatworm.indexing.data;

public class VarcharData extends StringData {
	
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

	private String c;
	private VarcharType type;
	
	@Override
	public String toString() {
		return c;
	}
	
}
