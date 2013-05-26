package fatworm.indexing.data;

public abstract class DataType {

	public abstract int storageRequired();
	
	public abstract Data getDefaultValue();
	
	public abstract Data valueOf(String c);
	
	public abstract Data valueOf(Data data) throws Exception;
	
	@Override
	public abstract String toString();

	protected static final int BOOLEAN_STORAGE_REQUIRED = 1;
	protected static final int CHAR_STORAGE_REQUIRED = -1;
	protected static final int DATETIME_STORAGE_REQUIRED = 10;
	protected static final int DECIMAL_STORAGE_REQUIRED = -1;
	protected static final int FLOAT_STORAGE_REQUIRED = 4;
	protected static final int INTEGER_STORAGE_REQUIRED = 4;
	protected static final int TIMESTAMP_STORAGE_REQUIRED = 30;
	protected static final int VARCHAR_STORAGE_REQUIRED = -1;
}
