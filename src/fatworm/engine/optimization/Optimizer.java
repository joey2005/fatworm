package fatworm.engine.optimization;

import java.util.*;
import fatworm.engine.plan.*;
import fatworm.engine.predicate.*;
import fatworm.engine.symbol.Symbol;
import fatworm.indexing.schema.Schema;

public class Optimizer {
	
	private Plan plan;

	public Optimizer(Plan p) {
		this.plan = p;
	}
	
	public Plan optimize() {
		if (plan instanceof CreateDatabasePlan) {
			return plan;
		}
		if (plan instanceof CreateIndexPlan) {
			return plan;
		}
		if (plan instanceof CreateTablePlan) {
			return plan;
		}
		if (plan instanceof DeletePlan) {
			return plan;
		}
		if (plan instanceof DropDatabasePlan) {
			return plan;
		}
		if (plan instanceof DropIndexPlan) {
			return plan;
		}
		if (plan instanceof DropTablePlan) {
			return plan;
		}
		if (plan instanceof InsertValuePlan) {
			return plan;
		}
		if (plan instanceof UpdatePlan) {
			return plan;
		}
		if (plan instanceof UseDatabasePlan) {
			return plan;
		}
		try {
			return optimizeS(plan);
		} catch (Exception ex) {
			return plan;
		}
	}
	
	private Plan optimizeS(Plan plan) {
		if (plan instanceof InsertSubQueryPlan) {
			InsertSubQueryPlan iplan = (InsertSubQueryPlan) plan;
			Plan subplan = iplan.subPlan();
			return new InsertSubQueryPlan(iplan.tableName, optimizeS(subplan));
		}
		if (plan instanceof ProductPlan) {
			ProductPlan pplan = (ProductPlan) plan;
			Plan left = pplan.lhs;
			Plan right = pplan.rhs;
			return new ProductPlan(optimizeS(left), optimizeS(right));
		}
		if (plan instanceof ProjectPlan) {
			ProjectPlan pplan = (ProjectPlan) plan;
			Plan subplan = pplan.subPlan();
			return new ProjectPlan(optimizeS(subplan), pplan.projectList, pplan.groupBy);
		}
		if (plan instanceof RenamePlan) {
			RenamePlan rplan = (RenamePlan) plan;
			Plan subplan = rplan.subPlan();
			return new RenamePlan(optimizeS(subplan), rplan.getSchema());
		}
		if (plan instanceof SortPlan) {
			SortPlan splan = (SortPlan) plan;
			Plan subplan = splan.subPlan();
			return new SortPlan(optimizeS(subplan), splan.colNameList, splan.orderList);
		}
		if (plan instanceof TablePlan) {
			return plan;
		}
		
		// SelectPlan()
		SelectPlan splan = (SelectPlan) plan;
		Plan subplan = splan.subPlan();
		if (splan.whereCondition == null) {
			return new SelectPlan(optimizeS(subplan), null);
		}
		
		Schema renamingSchema = null;
		
		while (subplan instanceof RenamePlan) {
			RenamePlan rplan = (RenamePlan) subplan;
			if (renamingSchema == null) {
				renamingSchema = rplan.getSchema();
			}
			subplan = rplan.subPlan();
		}
		if (subplan instanceof TablePlan) {
			return splan;
		}
		
		Set<String> colnameSet = getColName(splan.whereCondition);
		
		//push splan.whereCondition into subplan
		
		boolean push = true;
		Plan newSubplan = null;
		if (subplan instanceof ProductPlan) {
			ProductPlan pplan = (ProductPlan) subplan;
			Plan left = pplan.lhs;
			Plan right = pplan.rhs;
			Schema leftSchema = left.getSchema();
			Schema rightSchema = right.getSchema();
			if (testContains(leftSchema, colnameSet)) {
				SelectPlan newLeftplan = new SelectPlan(left, splan.whereCondition);
				left = optimizeS(newLeftplan);
			} else {
				push = false;
			}
			if (testContains(rightSchema, colnameSet)) {
				SelectPlan newRightplan = new SelectPlan(right, splan.whereCondition);
				right = optimizeS(newRightplan);
			} else {
				push = false;
			}
			newSubplan = new ProductPlan(left, right);
		}
		if (subplan instanceof ProjectPlan) {// may be the case group by and having ?
			ProjectPlan pplan = (ProjectPlan) subplan;
			Plan tmpplan = pplan.subPlan();
			Schema tmpSchema = tmpplan.getSchema();
			if (pplan.groupBy != null) {
				tmpplan = optimizeS(tmpplan);
				push = false;
			} else {
				if (testContains(tmpSchema, colnameSet)) {
					SelectPlan newTmpplan = new SelectPlan(tmpplan, splan.whereCondition);
					tmpplan = optimizeS(newTmpplan);
				} else {
					push = false;
				}
			}
			newSubplan = new ProjectPlan(tmpplan, pplan.projectList, pplan.groupBy);
		}
		if (subplan instanceof SelectPlan) {
			SelectPlan selectplan = (SelectPlan) subplan;
			Plan tmpplan = selectplan.subPlan();
			Schema tmpSchema = tmpplan.getSchema();
			if (testContains(tmpSchema, colnameSet)) {
				Predicate testCondition = splan.whereCondition;
				if (selectplan.whereCondition != null) {
					testCondition = new BooleanPredicate(splan.whereCondition, selectplan.whereCondition, Symbol.AND);
				}
				SelectPlan newTmpplan = new SelectPlan(tmpplan, testCondition);
				tmpplan = optimizeS(newTmpplan);
			} else {
				push = false;
			}
			if (push) {
				newSubplan = new SelectPlan(tmpplan, null);
			} else {
				newSubplan = new SelectPlan(tmpplan, selectplan.whereCondition);
			}
		}
		if (subplan instanceof SortPlan) {
			SortPlan sortplan = (SortPlan) subplan;
			Plan tmpplan = sortplan.subPlan();
			Schema tmpSchema = tmpplan.getSchema();
			if (testContains(tmpSchema, colnameSet)) {
				SelectPlan newTmpplan = new SelectPlan(tmpplan, splan.whereCondition);
				tmpplan = optimizeS(newTmpplan);				
			} else {
				push = false;
			}
			newSubplan = new SortPlan(tmpplan, sortplan.colNameList, sortplan.orderList);
		} else {
			newSubplan = subplan;
		}
		
		Plan result = null;
		if (renamingSchema != null) {
			result = new RenamePlan(newSubplan, renamingSchema);
		} else {
			result = newSubplan;
		}
		if (!push) {
			result = new SelectPlan(result, splan.whereCondition);
		}
		
		return result;
	}
	
	Set<String> getColName(Predicate p) {
		Set<String> result = new HashSet<String>();
		if (p instanceof AllPredicate) {
			AllPredicate ap = (AllPredicate) p;
			result = getColName(ap.value);
		}
		if (p instanceof AnyPredicate) {
			AnyPredicate ap = (AnyPredicate) p;
			result = getColName(ap.value);
		}
		if (p instanceof BooleanCompPredicate) {
			BooleanCompPredicate bcp = (BooleanCompPredicate) p;
			result.addAll(getColName(bcp.lhs));
			result.addAll(getColName(bcp.rhs));
		}
		if (p instanceof BooleanPredicate) {
			BooleanPredicate bp = (BooleanPredicate) p;
			result.addAll(getColName(bp.lhs));
			result.addAll(getColName(bp.rhs));
		}
		if (p instanceof FuncPredicate) {
			FuncPredicate fp = (FuncPredicate) p;
			result = getColName(fp.colName);
		}
		if (p instanceof InPredicate) {
			InPredicate ip = (InPredicate) p;
			result = getColName(ip.value);
		}
		if (p instanceof NumberCalcPredicate) {
			NumberCalcPredicate ncp = (NumberCalcPredicate) p;
			result.addAll(getColName(ncp.lhs));
			result.addAll(getColName(ncp.rhs));
		}
		if (p instanceof VariablePredicate) {
			VariablePredicate vp = (VariablePredicate) p;
			result.add(vp.variableName);
		}
		return result;
	}
	
	boolean testContains(Schema s, Set<String> colnameSet) {
		for (String colname : colnameSet) {
			if (s.indexOf(colname) < 0) {
				return false;
			}
		}
		return true;
	}
}
