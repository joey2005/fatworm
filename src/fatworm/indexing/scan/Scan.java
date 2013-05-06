package fatworm.indexing.scan;

import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;

public abstract class Scan {
	
	public abstract boolean hasNext();

	public abstract Record next();
	
	public abstract Schema getSchema();
	
	public abstract void beforeFirst();
	
	public abstract void close();
}
