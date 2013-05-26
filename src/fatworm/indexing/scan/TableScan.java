package fatworm.indexing.scan;

import fatworm.indexing.LogicalFileMgr;
import fatworm.indexing.metadata.TableInfo;
import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;
import fatworm.indexing.table.RecordFile;
import fatworm.indexing.table.TableFile;
import fatworm.util.Fatworm;

public class TableScan extends Scan {
	
	private TableInfo ti;
	private TableFile tf;
	private Record next;
	
	public TableScan(String tableName) {
		this.ti = Fatworm.metadataMgr().getTableInfo(tableName);
		this.tf = Fatworm.metadataMgr().getTableAccess(tableName);
		beforeFirst();
	}

	@Override
	public boolean hasNext() {
		if (next == null) {
			if (tf.hasNext()) {
				next = tf.next();
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
		return ti.schema();
	}

	@Override
	public void beforeFirst() {
		tf.beforeFirst();
		next = null;
	}

	@Override
	public void close() {
		next = null;
		tf.close();
	}

	@Override
	public String toString() {
		return "table scan()";
	}

}
