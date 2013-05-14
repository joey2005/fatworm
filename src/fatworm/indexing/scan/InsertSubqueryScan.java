package fatworm.indexing.scan;

import fatworm.engine.plan.Plan;
import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;

public class InsertSubQueryScan extends Operation {
	
	private String tableName;
	private Scan scan;
	
	public InsertSubQueryScan(String tableName, Scan scan) {
		this.tableName = tableName;
		this.scan = scan;
	}

	@Override
	public void doit() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		scan.close();
		tableName = null;
	}

}
