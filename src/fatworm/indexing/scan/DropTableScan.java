package fatworm.indexing.scan;

import java.util.List;

import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;
import fatworm.util.Fatworm;

public class DropTableScan extends Operation {
	
	private List<String> tableNameList;
	
	public DropTableScan(List<String> tableNameList) {
		this.tableNameList = tableNameList;
	}

	@Override
	public void doit() {
		Fatworm.tx.tableMgr.dropTable(tableNameList);
	}

	@Override
	public void close() {
		tableNameList = null;
	}

	@Override
	public String toString() {
		return "drop table scan()";
	}

}
