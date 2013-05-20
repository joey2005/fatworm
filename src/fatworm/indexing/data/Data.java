package fatworm.indexing.data;

public abstract class Data implements Comparable<Data> {
	
	public abstract boolean isNull();
	
	public abstract DataType getType();
	
	@Override
	public abstract String toString();
	
	public abstract Object getValue();
	
	@Override
	public abstract int compareTo(Data o);
	
	@Override
	public abstract boolean equals(Object obj);
}
