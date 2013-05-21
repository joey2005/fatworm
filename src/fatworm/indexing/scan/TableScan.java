package fatworm.indexing.scan;

import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;
import fatworm.util.Fatworm;

public class TableScan extends Scan {
	
	private String tableName;
	private Schema schema;
	private int pointer;
	private Record next;
	
	public TableScan(String tableName) {
		this.tableName = tableName;
		this.schema = null;
		beforeFirst();
	}

	@Override
	public boolean hasNext() {
		if (next == null) {
			if (pointer < Fatworm.tx.tableMgr.getTable(tableName).size()) {
				next = Fatworm.tx.tableMgr.getRecordFromTable(tableName, pointer++);
			}
		}
		return next != null;
	}

	@Override
	public Record next() {
		Record result = next;
		next = null;
		return result;
	}

	@Override
	public Schema getSchema() {
		if (schema == null) {
			schema = Fatworm.tx.infoMgr.getSchema(tableName);
		}
		return schema;
	}

	@Override
	public void beforeFirst() {
		next = null;
		pointer = 0;
	}

	@Override
	public void close() {
		next = null;
		tableName = null;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return null;
	}

}
