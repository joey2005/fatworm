package fatworm.indexing.scan;

import fatworm.engine.plan.Plan;
import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;
import fatworm.util.Fatworm;

public class InsertSubQueryScan extends Operation {
	
	private String tableName;
	private Scan scan;
	
	public InsertSubQueryScan(String tableName, Scan scan) {
		this.tableName = tableName;
		this.scan = scan;
	}

	@Override
	public void doit() {
		scan.beforeFirst();
		while (scan.hasNext()) {
			Record next = scan.next();
			Fatworm.tx.tableMgr.addRecord(tableName, next);
		}
	}

	@Override
	public void close() {
		if (scan != null) {
			scan.close();
			scan = null;
		}
		tableName = null;
	}

	@Override
	public String toString() {
		return "insert subquery scan(" + scan.toString() + ")";
	}

}
