package fatworm.indexing.scan;

import java.util.ArrayList;
import java.util.List;

import fatworm.indexing.table.Record;
import fatworm.indexing.table.TableFile;
import fatworm.util.Fatworm;

public class InsertSubQueryScan extends Operation {
	
	private TableFile tf;
	private Scan scan;
	
	public InsertSubQueryScan(String tableName, Scan scan) {
		this.tf = Fatworm.metadataMgr().getTableAccess(tableName);
		this.scan = scan;
	}

	@Override
	public void doit() {
		List<Record> records = new ArrayList<Record>();
		scan.beforeFirst();
		while (scan.hasNext()) {
			Record next = scan.next();
			records.add(next);
		}
		for (Record record : records) {
			tf.insertRecord(record);
		}
		tf.close();
	}

	@Override
	public void close() {
		if (scan != null) {
			scan.close();
			scan = null;
		}
	}

	@Override
	public String toString() {
		return "insert subquery scan(" + scan.toString() + ")";
	}

}
