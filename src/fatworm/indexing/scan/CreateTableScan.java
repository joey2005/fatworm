package fatworm.indexing.scan;

import java.util.List;

import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;

public class CreateTableScan extends Operation {
	
	private Schema schema;
	private List<String> primaryKeys;
	
	public CreateTableScan(Schema schema, List<String> primaryKeys) {
		this.schema = schema;
		this.primaryKeys = primaryKeys;
	}

	/**
	 * create table
	 */
	@Override
	public void doit() {
		
	}

	@Override
	public void close() {
		schema = null;
		primaryKeys = null;
	}

}
