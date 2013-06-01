package fatworm.indexing.scan;

import fatworm.indexing.table.Record;
import fatworm.indexing.table.TableFile;
import fatworm.util.Fatworm;

public class InsertValueScan extends Operation {
	
	private TableFile tf;
	private Record record;
	
	public InsertValueScan(String tableName, Record record) {
		this.tf = Fatworm.metadataMgr().getTableAccess(tableName);
		this.record = record;
	}
	
	/**
	 * insert a record into table
	 */
	public void doit() {
		tf.insertRecord(record);
		tf.close();
	}

	@Override
	public void close() {
		record = null;
	}

	@Override
	public String toString() {
		return "insert value scan()";
	}
}
