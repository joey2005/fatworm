package fatworm.indexing.scan;

import java.util.List;

import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;

public class SortScan extends Scan {
	
	private Scan scan;
	private Schema schema;
	private List<Order> orders;
	
	public SortScan(Scan scan, List<Order> orders) {
		this.scan = scan;
		this.schema = scan.getSchema();
		this.orders = orders;
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Record next() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Schema getSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void beforeFirst() {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		if (scan != null) {
			scan.close();
			scan = null;
		}
		orders = null;
	}

	public static class Order {
		
		private String colName;
		private boolean ascending;
		
		public Order(String colName, boolean ascending) {
			this.colName = colName;
			this.ascending = ascending;
		}
	}

	@Override
	public String toString() {
		return "sort scan(" + scan.toString() + ")";
	}
}
