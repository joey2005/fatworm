package fatworm.indexing.scan;

import java.util.*;

import fatworm.engine.predicate.FuncPredicate;
import fatworm.engine.predicate.Predicate;
import fatworm.indexing.data.Data;
import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;

public class ProjectScan extends Scan {
	
	private class NormalScan extends Scan {
		
		private Scan scan;
		private Record next;
		
		public NormalScan(Scan s) {
			this.scan = s;
		}

		@Override
		public boolean hasNext() {
			if (scan == null) {
				return false;
			}
			if (next == null) {
				if (!scan.hasNext()) {
					return false;
				}
				Record record = scan.next();
					
				List<Data> datas = new ArrayList<Data>();
				for (Predicate p : pList) {
					datas.add(p.calc(record));
				}
				next = new Record(datas, getSchema());
			}
			return true;
		}

		@Override
		public Record next() {
			Record result = next;
			next = null;
			return result;
		}

		@Override
		public Schema getSchema() {
			return ProjectScan.this.getSchema();
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
		}

		@Override
		public String toString() {
			return "normal scan(" + scan.toString() + ")";
		}
		
	}
	
	private class SingleScan extends Scan {
		
		private Scan scan;
		private Record next;
		
		public SingleScan(Scan s) {
			scan = s;
			prepare();
		}
		
		private void prepare() {
			List<Record> sublists = new ArrayList<Record>();
			scan.beforeFirst();
			while (scan.hasNext()) {
				sublists.add(scan.next());
			}
			
			for (Predicate p : pList) {
				if (p instanceof FuncPredicate) {
					FuncPredicate fp = (FuncPredicate) p;
					fp.prepare(sublists);
				}
			}
			
			List<Data> datas = new ArrayList<Data>();
			for (Predicate p : pList) {
				if (sublists.isEmpty()) {
					datas.add(p.calc(null));
				} else {
					datas.add(p.calc(sublists.get(0)));
				}
			}
			
			next = new Record(datas, getSchema());
		}

		@Override
		public boolean hasNext() {
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
			return ProjectScan.this.getSchema();
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
			next = null;
		}

		@Override
		public String toString() {
			return "single scan(" + scan.toString() + ")";
		}
		
	}
	
	private class GroupByScan extends Scan {

		private Scan scan;
		private Record next, last;
		private boolean start;
		private boolean finish;
		
		public GroupByScan(Scan s) {
			List<SortScan.Order> orders = new ArrayList<SortScan.Order>();
			orders.add(new SortScan.Order(groupBy, true));
			
			if (s != null) {
				scan = new SortScan(s, orders);
			} else {
				scan = null;
			}
		}

		@Override
		public boolean hasNext() {
			if (scan == null) {
				return false;
			}
			if (next == null) {
				if (finish) {
					return false;
				}
				if (!start) {
					start = true;
					if (!scan.hasNext()) {
						return false;
					}
					last = scan.next();
				}
				
				List<Record> sublists = new ArrayList<Record>();
				sublists.add(last);
				finish = true;
				while (scan.hasNext()) {
					Record now = scan.next();
					finish = false;
					if (now.getFromVariableName(groupBy).compareTo(
							last.getFromVariableName(groupBy)) != 0) {
						last = now;
						break;
					}
					sublists.add(now);
				}
				if (finish) {
					return false;
				}

				for (Predicate p : pList) {
					if (p instanceof FuncPredicate) {
						FuncPredicate fp = (FuncPredicate) p;
						fp.prepare(sublists);
					}
				}
				
				List<Data> datas = new ArrayList<Data>();
				for (Predicate p : pList) {
					if (sublists.isEmpty()) {
						datas.add(p.calc(null));
					} else {
						datas.add(p.calc(sublists.get(0)));
					}
				}
				
				next = new Record(datas, getSchema());
			}
			return true;
		}

		@Override
		public Record next() {
			Record result = next;
			next = null;
			return result;
		}

		@Override
		public Schema getSchema() {
			return ProjectScan.this.getSchema();
		}

		@Override
		public void beforeFirst() {
			if (scan != null) {
				scan.beforeFirst();
			}
			next = null;
			start = false;
			finish = false;
		}

		@Override
		public void close() {
			if (scan != null) {
				scan.close();
				scan = null;
			}
			next = null;
		}

		@Override
		public String toString() {
			return "group by scan(" + scan.toString() + ")";
		}
		
	}
	
	private Scan scan;
	private Schema schema;
	private List<Predicate> pList;
	private String groupBy;
	
	public ProjectScan(Scan s, Schema schema, List<Predicate> pList, String groupBy) {
		this.schema = schema;
		this.pList = pList;
		this.groupBy = groupBy;
		
		if (s != null) {
			if (groupBy == null) {
				if (hasFunction()) {
					scan = new SingleScan(s);
				} else {
					scan = new NormalScan(s);
				}
			} else {
				scan = new GroupByScan(s);
			}
		} else {
			scan = null;
		}
	}
	
	private boolean hasFunction() {
		for (Predicate p : pList) {
			if (p.existsFunction()) {
				return true;
			}
		}
		return false;
	}
	
	private Record next;

	@Override
	public boolean hasNext() {
		if (scan == null) {
			return false;
		}
		if (next == null) {
			if (!scan.hasNext()) {
				return false;
			}
			next = scan.next();
		}
		return true;
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
		pList = null;
		groupBy = null;
		next = null;
	}

	@Override
	public String toString() {
		return "project scan(" + scan.toString() + ")";
	}
}
