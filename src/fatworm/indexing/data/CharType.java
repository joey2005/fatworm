package fatworm.indexing.data;

public class CharType extends DataType {
	
	public int length;
	
	public CharType(int length) {
		this.length = length;
	}

	@Override
	public int storageRequired() {
		return length;
	}

	@Override
	public Data getDefaultValue() {
		if (defaultValue == null) {
			String c = "";
			for (int i = 0; i < length; ++i) {
				c += " ";
			}
			defaultValue = new CharData(c, this); 
		}
		return defaultValue;
	}

	@Override
	public String toString() {
		return "Char(" + length + ")";
	}

	@Override
	public Data valueOf(String c) {
		return new CharData(c, this);
	}

	@Override
	public Data valueOf(Data data) throws Exception {
		if (data instanceof StringData) {
			return valueOf((StringData)data.getValue());
		}
		throw new Exception("Data Format Wrong");
	}

	private CharData defaultValue = null;

	@Override
	public int encode() {
		return setFirstArg(CHAR, length);
	} 
}
