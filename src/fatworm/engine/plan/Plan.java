package fatworm.engine.plan;

import fatworm.indexing.scan.*;

public abstract class Plan {
	
	public abstract int getPlanID();
	
	public abstract Scan createScan();
	
	public static int planCount = 0;
	
}
