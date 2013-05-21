package fatworm.engine.plan;

import fatworm.indexing.data.DataType;
import fatworm.indexing.scan.ProjectScan;
import fatworm.indexing.scan.Scan;
import fatworm.indexing.schema.AttributeField;
import fatworm.indexing.schema.Schema;

import java.util.ArrayList;
import java.util.List;

import fatworm.engine.predicate.*;

public class ProjectPlan extends Plan {

	public Plan subPlan;
	public List<Predicate> projectList;
	public int planID;
	public String groupBy;
	public Schema schema;
	
	public ProjectPlan(Plan subPlan, List<Predicate> pList, String groupBy) {
		this.subPlan = subPlan;
		this.projectList = pList;
		this.groupBy = groupBy;
		this.schema = calcSchema();
	}
	
	public Schema calcSchema() {
		List<AttributeField> fields = new ArrayList<AttributeField>();
		String tableName = subPlan == null ? "" : subPlan.getSchema().getTableName();
		for (Predicate p : projectList) {
			if (p instanceof FuncPredicate) {
				FuncPredicate fp = (FuncPredicate)p;
				DataType type = subPlan.getSchema().getFromVariableName(fp.colName.toString()).getType();
				fields.add(new AttributeField(tableName + "." + fp.toString(), type, -1, null, false));
			} else if (p instanceof VariablePredicate) {
				VariablePredicate vp = (VariablePredicate)p;
				DataType type = subPlan.getSchema().getFromVariableName(vp.variableName).getType();
				fields.add(new AttributeField(tableName + "." + vp.toString(), type, -1, null, false));			
			} else {
				// impossible ? 
			}
		}
		return new Schema(tableName, fields);
	}
	
	@Override
	public String toString() {
		String result = "( " + subPlan.toString() + " )\n";
		result += "Plan #" + (planID = Plan.planCount++) + " <- " + "Project Plan #" + subPlan.getPlanID() + " into columns( ";
		for (int i = 0; i < projectList.size(); ++i) {
			result += projectList.get(i).toString() + ", ";
		}
		result += ")";
		if (groupBy != null) {
			result += " GROUP BY " + groupBy;
		}
		return result;
	}

	@Override
	public int getPlanID() {
		return planID;
	}

	@Override
	public Scan createScan() {
		return new ProjectScan(subPlan.createScan(), schema, projectList, groupBy);
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
