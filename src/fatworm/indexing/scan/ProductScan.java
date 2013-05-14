package fatworm.indexing.scan;

import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;

public class ProductScan extends Scan {
	
	private Scan s1, s2;
	private Record next, left, right;
	private boolean start;
	private Schema schema;
	
	public ProductScan(Scan s1, Scan s2, Schema schema) {
		this.s1 = s1;
		this.s2 = s2;
		this.schema = schema;
		beforeFirst();
	}

	@Override
	public boolean hasNext() {
		if (next == null) {
			if (!start) {
				s1.beforeFirst();
				s2.beforeFirst();
				if (s1.hasNext()) {
					left = s1.next();
				}
				right = null;
				start = true;
			}
			if (s2.hasNext()) {
				right = s2.next();
				next = union(left, right);
			} else {
				if (s1.hasNext()) {
					left = s1.next();
					s2.beforeFirst();
					if (s2.hasNext()) {
						right = s2.next();
						next = union(left, right);
					}
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

	private Record union(Record left, Record right) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void beforeFirst() {
		s1.beforeFirst();
		s2.beforeFirst();
		left = right = next = null;
		start = false;
	}

	@Override
	public void close() {
		s1.close();
		s2.close();
		left = right = null;
		s1 = s2 = null;
	}

}
