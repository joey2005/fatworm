package fatworm.engine.plan;

import fatworm.indexing.scan.Scan;
import fatworm.indexing.scan.SortScan;
import fatworm.indexing.schema.Schema;

import java.util.LinkedList;
import java.util.List;

public class SortPlan extends Plan {
	
	public Plan tablePlan;
	public Schema schema;
	public List<String> colNameList;
	public List<Boolean> orderList;
	public int planID;
	
	public SortPlan(Plan tablePlan, List<String> colNameList, List<Boolean> orderList) {
		this.tablePlan = tablePlan;
		this.colNameList = colNameList;
		this.orderList = orderList;
		this.schema = tablePlan.getSchema();
	}
	
	@Override
	public String toString() {
		String result = tablePlan.toString() + "\n";
		result += "Plan #" + (planID = Plan.planCount++) + " <- " + "Sort Plan #" + tablePlan.getPlanID() + " as:\n";
		for (int i = 0; i < colNameList.size(); ++i) {
			result += "sort " + colNameList.get(i) + " by ";
			if (orderList.get(i)) {
				result += "Ascending";
			} else {
				result += "Descending";
			}
			result += "\n";
		}
		return result;
	}

	@Override
	public int getPlanID() {
		return planID;
	}

	@Override
	public Scan createScan() {
		List<SortScan.Order> orders = new LinkedList<SortScan.Order>();
		for (int i = 0; i < colNameList.size(); ++i) {
			orders.add(new SortScan.Order(colNameList.get(i), orderList.get(i)));
		}
		return new SortScan(tablePlan.createScan(), orders);
	}

	@Override
	public Plan subPlan() {
		return tablePlan;
	}

	@Override
	public Schema getSchema() {
		return schema;
	}
}
