package fatworm.indexing.scan;

import java.util.List;

import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;

public class SortScan extends Scan {
	
	private Scan scan;
	private Schema schema;
	private List<Order> sorts;
	
	public SortScan(Scan scan, List<Order> sorts) {
		this.scan = scan;
		this.schema = scan.getSchema();
		this.sorts = sorts;
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
		scan.close();
	}

	public static class Order {
		
		private String colName;
		private boolean ascending;
		
		public Order(String colName, boolean ascending) {
			this.colName = colName;
			this.ascending = ascending;
		}
	}
}
