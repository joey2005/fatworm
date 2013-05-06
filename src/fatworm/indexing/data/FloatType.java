package fatworm.indexing.data;

public class FloatType extends NumberType {

	public FloatType() {
	}

	@Override
	public Data getDefaultValue() {
		if (defaultValue == null) {
			defaultValue = new FloatData(Float.valueOf(0.0f), this);
		}
		return defaultValue;
	}
	
	@Override
	public Data valueOf(Data data) throws Exception {
		if (data instanceof NumberData) {
			NumberData num = (NumberData) data;
			return new FloatData(num.FloatValue(), this);
		}
		throw new Exception("Data Format Wrong");
	}
	
	@Override
	public int storageRequired() {
		return FLOAT_STORAGE_REQUIRED;
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof FloatType;
	}

	@Override
	public String toString() {
		return "Float";
	}

	@Override
	public Data valueOf(String value) {
		return new FloatData(value, this);
	}
	
	private FloatData defaultValue = null;
}
