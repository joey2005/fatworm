package fatworm.indexing.data;

public abstract class DataType {
	
	public abstract int encode();
	
	public static DataType decode(int code) {
		switch (getType(code)) {
		case BOOLEAN:
			return new BooleanType();
		case CHAR:
			return new CharType(getFirstArg(code));
		case DATETIME:
			return new DateTimeType();
		case DECIMAL:
			return new DecimalType(getFirstArg(code), getSecondArg(code));
		case FLOAT:
			return new FloatType();
		case INTEGER:
			return new IntegerType();
		case TIMESTAMP:
			return new TimestampType();
		case VARCHAR:
			return new VarcharType(getFirstArg(code));
		default:
			return null;	
		}
	}

	private static int getType(int code) {
		return code & 0x7f000000;
	}

	private static int getFirstArg(int code) {
		return (code & 0x00fff000) >> 12;
	}

	private static int getSecondArg(int code) {
		return (code & 0x00000fff);
	}

	protected static int setFirstArg(int code, int arg) {
		return code | (arg << 12);
	}

	protected static int setSecondArg(int code, int arg) {
		return code | arg;
	}

	protected static final int BOOLEAN = (0 << 24);
	protected static final int CHAR = (1 << 24);
	protected static final int DATETIME = (2 << 24);
	protected static final int DECIMAL = (3 << 24);
	protected static final int FLOAT = (4 << 24);
	protected static final int INTEGER = (5 << 24);
	protected static final int TIMESTAMP = (6 << 24);
	protected static final int VARCHAR = (7 << 24);

	public abstract int storageRequired();
	
	public abstract Data getDefaultValue();
	
	public abstract Data valueOf(String c);
	
	public abstract Data valueOf(Data data) throws Exception;
	
	@Override
	public abstract String toString();

	protected static final int BOOLEAN_STORAGE_REQUIRED = 5;
	protected static final int CHAR_STORAGE_REQUIRED = -1;
	protected static final int DATETIME_STORAGE_REQUIRED = 30;
	protected static final int DECIMAL_STORAGE_REQUIRED = -1;
	protected static final int FLOAT_STORAGE_REQUIRED = 20;
	protected static final int INTEGER_STORAGE_REQUIRED = 10;
	protected static final int TIMESTAMP_STORAGE_REQUIRED = 30;
	protected static final int VARCHAR_STORAGE_REQUIRED = -1;
}
