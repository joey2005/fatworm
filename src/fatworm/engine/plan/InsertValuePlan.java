package fatworm.engine.plan;

import fatworm.indexing.data.Data;
import fatworm.indexing.scan.InsertValueScan;
import fatworm.indexing.scan.Scan;
import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;

import java.util.List;

import fatworm.engine.predicate.*;

public class InsertValuePlan extends Plan {
	
	private String tableName;
	private Schema schema;
	private List<Predicate> list;
	private int planID;
	
	public InsertValuePlan(String tableName, List<Predicate> list, List<String> columns) {
		this.tableName = tableName;
		this.list = list;
		
		if (columns == null) {
			//read the schema of this table into this.schema
		}
	}
	
	@Override
	public String toString() {
		String result = "insert into " + tableName + ": value( ";
		for (Predicate p : list) {
			result += p.toString() + ", ";
		}
		result += " )";
		return "Plan #" + (planID = Plan.planCount++) + " <- " + result;
	}

	@Override
	public int getPlanID() {
		return planID;
	}

	@Override
	public Scan createScan() {
		List<Data> datas = null;
		return new InsertValueScan(tableName, new Record(datas, schema));
	}

	@Override
	public Plan subPlan() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Schema getSchema() {
		return schema;
	}
}
