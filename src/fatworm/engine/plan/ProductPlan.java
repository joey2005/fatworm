package fatworm.engine.plan;

import fatworm.indexing.scan.ProductScan;
import fatworm.indexing.scan.Scan;
import fatworm.indexing.schema.Schema;

public class ProductPlan extends Plan {

	private Plan lhs, rhs;
	private Schema schema;
	private int planID;
	
	public ProductPlan(Plan lhs, Plan rhs) {
		this.lhs = lhs;
		this.rhs = rhs;
		Schema left = lhs.getSchema();
		Schema right = rhs.getSchema();
		this.schema = left.union(right, left.getTableName() + "X" + right.getTableName());
	}
	
	@Override
	public String toString() {
		String result = "( " + lhs.toString() + ")\n";
		result += "( " + rhs.toString() + "\n)";
		result += "Plan #" + (planID = Plan.planCount++) + " <- " + "Plan #" + lhs.getPlanID() + " X " + "Plan %" + rhs.getPlanID();
		return result;
	}

	@Override
	public int getPlanID() {
		return planID;
	}

	@Override
	public Scan createScan() {
		return new ProductScan(lhs.createScan(), rhs.createScan(), schema);
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
