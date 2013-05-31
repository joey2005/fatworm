package fatworm.indexing.scan;

import fatworm.indexing.LogicalFileMgr;
import fatworm.indexing.metadata.TableInfo;
import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;
import fatworm.indexing.table.RecordFile;
import fatworm.indexing.table.TableFile;
import fatworm.util.Fatworm;

import java.util.*;

public class TableScan extends Scan {
	
	private TableInfo ti;
	private TableFile tf;
	private Record next;
	private List<Record> table;
	private int ptr;
	
	public TableScan(String tableName) {
		this.ti = Fatworm.metadataMgr().getTableInfo(tableName);
		this.tf = Fatworm.metadataMgr().getTableAccess(tableName);
		
		prepare();
	}
	
	private void prepare() {
		tf.beforeFirst();
		table = tf.records();
		ptr = 0;
	}

	@Override
	public boolean hasNext() {
		if (next == null) {
			if (ptr < table.size()) {
				next = table.get(ptr++);
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
		ptr = 0;
		next = null;
	}

	@Override
	public void close() {
		next = null;
		table.clear();
		tf.close();
	}

	@Override
	public String toString() {
		return "table scan()";
	}

}
