package fatworm.indexing.scan;

import java.util.*;

import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;

public class SortScan extends Scan {
	
	public static class Order {
		
		public String colName;
		public boolean ascending;
		public int columnIndex;
		
		public Order(String colName, boolean ascending) {
			this.colName = colName;
			this.ascending = ascending;
		}
		
		public void prepare(Schema schema) {
			this.columnIndex = schema.indexOf(colName);
		}
	}

	@Override
	public String toString() {
		return "sort scan(" + scan.toString() + ")";
	}
	
	private Scan scan;
	private Schema schema;
	private List<Order> orders;
	private int pos;
	
	public SortScan(Scan scan, List<Order> orders) {
		this.scan = scan;
		this.schema = scan.getSchema();
		this.orders = orders;
		
		prepare();
	}
	
	private Record[] list, tmp;

	private void prepare() {
		for (Order order : orders) {
			order.prepare(schema);
		}
		ArrayList<Record> tmpList = new ArrayList<Record>();
		scan.beforeFirst();
		while (scan.hasNext()) {
			Record record = scan.next();
			tmpList.add(record);
		}
		
		list = new Record[tmpList.size()];
		for (int i = 0; i < list.length; ++i) {
			list[i] = tmpList.get(i);
		}
		tmp = new Record[list.length];
		
		qsort(0, list.length);
		
		this.next = null;
		this.pos = 0;
	}
	
	private void qsort(int left, int right) {
		if (left + 1 >= right) {
			return;
		}
		int mid = (left + right) / 2;
		qsort(left, mid);
		qsort(mid, right);
		int i = left, j = mid, ptr = left;
		while (i < mid && j < right) {
			if (compareTo(list[i], list[j]) < 0) {
				tmp[ptr++] = list[i++];
			} else {
				tmp[ptr++] = list[j++];
			}
		}
		while (i < mid) {
			tmp[ptr++] = list[i++];
		}
		while (j < right) {
			tmp[ptr++] = list[j++];
		}
		for (i = left; i < right; ++i) {
			list[i] = tmp[i];
		}
	}
	
	private int compareTo(Record r1, Record r2) {
		for (Order order : orders) {
			int result = r1.getFromColumn(order.columnIndex).compareTo(
					r2.getFromColumn(order.columnIndex));
			if (result != 0) {
				return order.ascending ? result : -result;
			}
		}
		return 0;
	}
	
	private Record next;
	
	@Override
	public boolean hasNext() {
		if (next == null) {
			if (pos < list.length) {
				next = list[pos++];
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
		prepare();
	}

	@Override
	public void close() {
		if (scan != null) {
			scan.close();
			scan = null;
		}
		orders = null;
	}

}
