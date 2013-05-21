package fatworm.indexing.scan;

import java.util.List;

import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;
import fatworm.util.Fatworm;

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
		System.out.println(schema.getTableName() + " " + schema.getColumnCount());
		Fatworm.tx.tableMgr.addTable(schema.getTableName());
		Fatworm.tx.infoMgr.addSchema(schema.getTableName(), schema);
	}

	@Override
	public void close() {
		schema = null;
		primaryKeys = null;
	}

	@Override
	public String toString() {
		return "create table scan()";
	}

}
