package fatworm.indexing.scan;

import fatworm.engine.predicate.Predicate;
import fatworm.indexing.data.BooleanData;
import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;

public class SelectScan extends Scan {
	
	private Scan scan;
	private Predicate whereCondition;
	private Record next;
	
	public SelectScan(Scan scan, Predicate whereCondition) {
		this.scan = scan;
		this.whereCondition = whereCondition;
		beforeFirst();
	}

	@Override
	public boolean hasNext() {
		if (scan == null) {
			return false;
		}
		if (next == null) {
			while (true) {
				if (scan.hasNext()) {
					next = scan.next();
					BooleanData ok;
					try {
						ok = whereCondition.test(next);
						if (!ok.isNull() && (Boolean)ok.getValue()) {
							break;
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					next = null;
				} else {
					break;
				}
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
		return scan.getSchema();
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
		next = null;
		whereCondition = null;
	}

	@Override
	public String toString() {
		return "select scan(" + scan.toString() + ")";
	}

}
