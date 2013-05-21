package fatworm.indexing.scan;

import java.util.ArrayList;

import fatworm.engine.predicate.Predicate;
import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;
import fatworm.util.Fatworm;

public class DeleteScan extends Operation {
	
	private String tableName;
	private Scan scan;
	
	public DeleteScan(String tableName, Scan scan) {
		this.tableName = tableName;
		this.scan = scan;
	}

	@Override
	public void doit() {
		if (scan == null) {
			return;
		}
		scan.beforeFirst();
		while (scan.hasNext()) {
			Record next = scan.next();
			Fatworm.tx.tableMgr.deleteRecord(tableName, next);
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
		return "delete table scan()";
	}

}
