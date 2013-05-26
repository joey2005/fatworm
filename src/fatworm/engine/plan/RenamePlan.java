package fatworm.engine.plan;

import java.util.*;

import fatworm.indexing.data.DataType;
import fatworm.indexing.scan.RenameScan;
import fatworm.indexing.scan.Scan;
import fatworm.indexing.schema.*;

public class RenamePlan extends Plan {

	public Plan subPlan;
	public Schema schema;
	public int planID;
	public String alias;
	
	public RenamePlan(Plan subPlan, String alias) {
		this.subPlan = subPlan;
		this.alias = alias;
		this.schema = translateSchema();
	}
	
	public RenamePlan(Plan subPlan, Schema schema) {
		this.subPlan = subPlan;
		this.alias = null;
		this.schema = schema;
	}
	
	private Schema translateSchema() {
		Schema s = subPlan.getSchema();
		List<AttributeField> attrList = new ArrayList<AttributeField>();
		for (int i = 0; i < s.getColumnCount(); ++i) {
			AttributeField af = s.getFromColumn(i);
			String fieldName = af.getColumnName();
			DataType type = af.getType();
			int dotpos = fieldName.indexOf(".");
			String colName = fieldName.substring(dotpos + 1);
			fieldName = alias + "." + colName;
			attrList.add(new AttributeField(fieldName, type, af.isNull, af.defaultValue, af.autoIncrement));
		}
		return new Schema(alias, attrList);
	}
	
	@Override
	public String toString() {
		String result = "( " + subPlan.toString() + " )\n";
		result += "Plan #" + (planID = Plan.planCount++) + " <- " + "Rename " + "Plan #" + subPlan.getPlanID() + " as " + alias;
		return result;
	}

	@Override
	public int getPlanID() {
		return planID;
	}

	@Override
	public Scan createScan() {
		return new RenameScan(subPlan.createScan(), schema);
	}

	@Override
	public Plan subPlan() {
		return subPlan;
	}

	@Override
	public Schema getSchema() {
		return schema;
	}
}
