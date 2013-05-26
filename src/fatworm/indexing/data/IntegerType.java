package fatworm.indexing.data;

public class IntegerType extends NumberType {

	public IntegerType() {
	}

	@Override
	public Data valueOf(Data data) throws Exception {
		if (data instanceof NumberData) {
			NumberData num = (NumberData) data;
			return new IntegerData(num.IntegerValue(), this);
		}
		throw new Exception("Data Format Wrong");
	}
	
	@Override
	public Data getDefaultValue() {
		return IntegerType.ZERO;
	}
	
	@Override
	public int storageRequired() {
		return INTEGER_STORAGE_REQUIRED;
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof IntegerType;
	}

	@Override
	public String toString() {
		return "Integer";
	}

	@Override
	public Data valueOf(String value) {
		return new IntegerData(value, this);
	}
	
	@Override
	public int encode() {
		return INTEGER;
	}

	public final static IntegerData ZERO = new IntegerData(0, new IntegerType()); 
	public final static IntegerData ONE = new IntegerData(1, new IntegerType());

}
