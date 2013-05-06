package fatworm.indexing.data;

public abstract class Data implements Comparable<Data> {
	
	public abstract boolean isNull();
	
	public abstract DataType getType();
	
	public abstract String toString();
	
	public abstract Object getValue();
}
