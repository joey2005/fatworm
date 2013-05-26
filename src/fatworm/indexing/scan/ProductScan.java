package fatworm.indexing.scan;

import fatworm.indexing.data.Data;
import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;

import java.util.*;


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
		if (s1 == null || s2 == null) {
			return false;
		}
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
		List<Data> datas = left.getData();
		datas.addAll(right.getData());
		return new Record(datas, schema);
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void beforeFirst() {
		if (s1 != null) {
			s1.beforeFirst();
		}
		if (s2 != null) {
			s2.beforeFirst();
		}
		left = right = next = null;
		start = false;
	}

	@Override
	public void close() {
		if (s1 != null) {
			s1.close();
			s1 = null;
		}
		if (s2 != null) {
			s2.close();
			s2 = null;
		}
		left = right = null;
	}

	@Override
	public String toString() {
		return "product scan(" + s1.toString() + ", " + s2.toString() + ")";
	}

}
