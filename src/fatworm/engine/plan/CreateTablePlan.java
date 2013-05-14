package fatworm.engine.plan;

import fatworm.indexing.scan.Scan;
import fatworm.indexing.schema.Attribute;
import fatworm.indexing.schema.Schema;

import java.util.List;

public class CreateTablePlan extends Plan {
	
	private Schema schema;
	private List<String> primaryKeys;
	private int planID;
	
	public CreateTablePlan(Schema schema, List<String> primaryKeys) {
		this.schema = schema;
		this.primaryKeys = primaryKeys;
	}
	
	@Override
	public String toString() {
		String result = "create table: " + schema.getTableName() + "\n(";
		for (int i = 0; i < schema.getColumnCount(); ++i) {
			Attribute at = schema.getFields(i);
			result += at.getColumnName() + ": " + at.getType().toString() + "\n";
		}
		result += "primary key: ";
		for (String c : primaryKeys) {
			result += c + ", ";
		}
		result += "\n)";
		result = "Plan #" + (planID = Plan.planCount++) + " <- " + result;
		return result;
	}

	@Override
	public int getPlanID() {
		return planID;
	}

	@Override
	public Scan createScan() {
		// TODO Auto-generated method stub
		return null;
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
