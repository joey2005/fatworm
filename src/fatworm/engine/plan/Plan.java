package fatworm.engine.plan;

import fatworm.indexing.scan.*;
import fatworm.indexing.schema.Schema;

public abstract class Plan {
	
	@Override
	public abstract String toString();
	
	public abstract int getPlanID();
	
	public abstract Scan createScan();
	
	public abstract Plan subPlan();
	
	public abstract Schema getSchema();
	
	public static int planCount = 0;
	
}
