package fatworm.indexing.data;

public class BooleanType extends DataType {

	@Override
	public int storageRequired() {
		return DataType.BOOLEAN_STORAGE_REQUIRED;
	}

	@Override
	public Data getDefaultValue() {
		return BooleanData.FALSE;
	}

	@Override
	public String toString() {
		return "Boolean";
	}

	@Override
	public Data valueOf(String c) {
		return new BooleanData(c, this);
	}

	@Override
	public Data valueOf(Data data) throws Exception {
		if (data instanceof BooleanData) {
			BooleanData bdata = (BooleanData)data;
			return new BooleanData((Boolean)bdata.getValue(), this);
		}
		throw new Exception("Data Format Wrong");
	}

}
