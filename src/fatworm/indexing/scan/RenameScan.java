package fatworm.indexing.scan;

import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;

public class RenameScan extends Scan {
	
	private Scan scan;
	private Schema schema;
	private Record next;
	
	public RenameScan(Scan scan, Schema schema) {
		this.scan = scan;
		this.schema = schema;
	}

	@Override
	public boolean hasNext() {
		if (scan == null) {
			return false;
		}
		if (next == null) {
			if (scan.hasNext()) {
				next = scan.next();
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
		return schema;
	}

	@Override
	public void beforeFirst() {
		if (scan != null) {
			scan.beforeFirst();
		}
		next = null;
	}

	@Override
	public void close() {
		if (scan != null) {
			scan.close();
			scan = null;
		}
		schema = null;
		next = null;
	}

	@Override
	public String toString() {
		return "rename scan(" + scan.toString() + ")";
	}

}
