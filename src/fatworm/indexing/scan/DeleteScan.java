package fatworm.indexing.scan;

import fatworm.engine.predicate.Predicate;
import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;

public class DeleteScan extends Operation {
	
	private String tableName;
	private Scan scan;
	
	public DeleteScan(String tableName, Scan scan) {
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

	@Override
	public String toString() {
		return "delete table scan()";
	}

}
