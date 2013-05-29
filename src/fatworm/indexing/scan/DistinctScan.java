package fatworm.indexing.scan;

import java.util.*;

import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;

public class DistinctScan extends Scan {
	
	private Scan scan; 
	private Record last, next;
	
	public DistinctScan(Scan scan) {
		List<SortScan.Order> sorts = new ArrayList<SortScan.Order>(); 
		Schema schema = scan.getSchema();
		int size = schema.getAllFields().size();
		for (int i = 0; i < size; ++i) {
			sorts.add(new SortScan.Order(schema.getFromColumn(i).getColumnName(), true));
		}
		
		this.scan = new SortScan(scan, sorts);
	}

	@Override
	public boolean hasNext() {
		if (scan == null) {
			return false;
		}
		if (next == null) {
			while (scan.hasNext()) {
				next = scan.next();
				if (last == null || !last.equals(next)) {
					break;
				}
				next = null;
			}
			last = next;
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
		last = next = null;
	}

	@Override
	public void close() {
		if (scan != null) {
			scan.close();
			scan = null;
		}
		last = next = null;
	}

	@Override
	public String toString() {
		return "distinct scan(" + scan.toString() + ")";
	}

}
