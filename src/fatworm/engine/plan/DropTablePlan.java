package fatworm.engine.plan;

import fatworm.indexing.scan.DropTableScan;
import fatworm.indexing.scan.Scan;
import fatworm.indexing.schema.Schema;

import java.util.List;

import org.antlr.runtime.tree.Tree;

public class DropTablePlan extends Plan {
	
	private List<String> tableNameList;
	private int planID;

	public DropTablePlan(List<String> tableNameList) {
		this.tableNameList = tableNameList;
	}

	@Override
	public String toString() {
		String result = "drop table: ";
		for (String tableName : tableNameList) {
			result += tableName + ", ";
		}
		return "Plan #" + (planID = Plan.planCount++) + " <- " +  result;	
	}

	@Override
	public int getPlanID() {
		return planID;
	}

	@Override
	public Scan createScan() {
		return new DropTableScan(tableNameList);
	}

	@Override
	public Plan subPlan() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Schema getSchema() {
		// TODO Auto-generated method stub
		return null;
	}

}
