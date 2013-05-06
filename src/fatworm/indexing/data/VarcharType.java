package fatworm.indexing.data;

public class VarcharType extends DataType {

	public VarcharType(int length) {
		this.length = length;
	}

	@Override
	public Data valueOf(Data data) throws Exception {
		if (data instanceof StringData) {
			StringData str = (StringData) data;
			return new VarcharData(str.getValue(), this);
		}
		throw new Exception("Data Format Wrong");
	}

	@Override
	public Data getDefaultValue() {
		if (defaultValue == null) {
			String c = "";
			for (int i = 0; i < length; ++i) {
				c += ' ';
			}
			defaultValue = new VarcharData(c, this);
		}
		return defaultValue;
	}

	@Override
	public int storageRequired() {
		return length;
	}

	@Override
	public String toString() {
		return "Varchar(" + length + ")";
	}
	
	@Override
	public Data valueOf(String value) {
		return new VarcharData(value, this);
	}

	public int getLength() {
		return length;
	}

	private int length;
	private VarcharData defaultValue = null;
}
