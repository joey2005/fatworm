package fatworm.indexing.scan;

import java.util.List;

import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;

public class DropTableScan extends Operation {
	
	private List<String> tableList;
	
	public DropTableScan(List<String> tableNameList) {
		this.tableList = tableNameList;
	}

	@Override
	public void doit() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		tableList = null;
	}

	@Override
	public String toString() {
		return "drop table scan()";
	}

}
