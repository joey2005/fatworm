package fatworm.engine.plan;

import fatworm.indexing.data.*;
import fatworm.indexing.schema.Attribute;
import fatworm.indexing.schema.Schema;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;

import fatworm.engine.predicate.*;
import fatworm.engine.symbol.Symbol;

public class Planner {
	
	private List<String> extractList(List<CommonTree> tokens) {
		List<String> result = new LinkedList<String>();
		for (CommonTree t : tokens) {
			result.add(t.toString());
		}
		//System.out.println(result.toString());
		return result;
	}
	
	/**
	 * return the schema of given table
	 * @param tblname
	 * @return
	 */
	private Schema getSchema(String tblname) {
		return null;
	}
	
	CommonTree tree;
	
	public Planner(CommonTree tree) {
		this.tree = tree;
	}

	public Plan generatePlan() throws Exception {
		switch (tree.getType()) {
		case Symbol.SELECT:
		case Symbol.SELECT_DISTINCT:
			return getSelect(tree);
		case Symbol.CREATE_DATABASE: 
			return getCreateDatabase(tree);
		case Symbol.USE_DATABASE:
			return getUseDatabase(tree);
		case Symbol.DROP_DATABASE:
			return getDropDatabase(tree);
		case Symbol.DELETE:
			return getDelete(tree);
		case Symbol.CREATE_INDEX:
		case Symbol.CREATE_UNIQUE_INDEX:
			return getCreateIndex(tree);
		case Symbol.DROP_INDEX:
			return getDropIndex(tree);
		case Symbol.INSERT_COLUMNS:
			return getInsertColumns(tree);
		case Symbol.INSERT_SUBQUERY:
			return getInsertSubquery(tree);
		case Symbol.INSERT_VALUES:
			return getInsertValues(tree);
		case Symbol.CREATE_TABLE:
			return getCreateTable(tree);
		case Symbol.DROP_TABLE:
			return getDropTable(tree);
		case Symbol.UPDATE:
			return getUpdate(tree);
		}
		return null;
	}

	private Plan getSelect(CommonTree t) throws Exception {//System.out.println(t.toStringTree());
		// work with select_expr
		Plan result = null;
		
		boolean selectAll = false;
		
		List<Predicate> colName = new ArrayList<Predicate>();
		List<String> alias = new ArrayList<String>();
		
		int i = 0;
		while (i < t.getChildCount()) {
			Tree cur = t.getChild(i);
			if (cur.getType() == Symbol.FROM ||
			cur.getType() == Symbol.WHERE ||
			cur.getType() == Symbol.HAVING ||
			cur.getType() == Symbol.ORDER ||
			cur.getType() == Symbol.GROUP) {
				// select_suffix*
				break;
			} else if (cur.getType() == Symbol.MUL) {
				// '*'
				selectAll = true;
			} else if (cur.getType() == Symbol.AS) {
				// value (AS^ alias)
				colName.add(translateValue(cur.getChild(0)));
				alias.add(cur.getChild(1).toString());
			} else {
				// value
				colName.add(translateValue(cur));
				alias.add(null);
			}
			++i;
		}
		
		if (selectAll && colName.size() > 0) {
			throw new Exception("Select Content Wrong!");
		}
		
		Plan fromResult = null;
		Predicate whereCondition = null;
		String groupBy = null;
		Predicate havingCondition = null;
		List<String> sortColumnList = null;
		List<Boolean> sortOrderList = null;
		
		while (i < t.getChildCount()) {
			Tree child = t.getChild(i);
			
			if (child.getType() == Symbol.FROM) {
				// from clause
				for (int pos = 0; pos < child.getChildCount(); ++pos) {
					// tbl_ref
					Plan tmp = null;
					Tree cur = child.getChild(pos);
					
					if (cur.getType() == Symbol.AS) {//System.out.println(cur.toStringTree());
						if (cur.getChild(0).getType() == Symbol.SELECT || cur.getChild(0).getType() == Symbol.SELECT_DISTINCT) {
							// subquery as alias
							tmp = new RenamePlan(getSubQuery(cur.getChild(0)), cur.getChild(1).toString());
						} else {
							String tblname = cur.getChild(0).toString();
							tmp = new RenamePlan(new TablePlan(tblname, getSchema(tblname)), cur.getChild(1).toString());
						}
					} else {
						String tblname = cur.getChild(0).toString();
						tmp = new TablePlan(tblname, getSchema(tblname));
					}
					if (fromResult == null) {
						fromResult = tmp;
					} else {
						fromResult = new ProductPlan(fromResult, tmp);
					}
				}
			} else if (child.getType() == Symbol.WHERE) {
				whereCondition = getCondition(child.getChild(0));
			} else if (child.getType() == Symbol.GROUP) {
				groupBy = translateColName(child.getChild(0));
			} else if (child.getType() == Symbol.HAVING) {
				havingCondition = getCondition(child.getChild(0));
			} else if (child.getType() == Symbol.ORDER) {
				if (sortColumnList == null) {
					sortColumnList = new LinkedList<String>();
					sortOrderList = new LinkedList<Boolean>();
					for (int pos = 0; pos < child.getChildCount(); ++pos) {
						Tree cur = child.getChild(pos);
						
						if (cur.getType() == Symbol.ASC) {
							sortColumnList.add( translateColName(cur.getChild(0)) );
							sortOrderList.add( Boolean.TRUE );							
						} else if (cur.getType() == Symbol.DESC) {
							sortColumnList.add( translateColName(cur.getChild(0)) );
							sortOrderList.add( Boolean.FALSE );								
						} else {
							sortColumnList.add( translateColName(cur) );
							sortOrderList.add( Boolean.TRUE );
						}
					}
				}
			}
			
			++i;
		}
		
		//where_condition
		result = new SelectPlan(fromResult, whereCondition);
		
		//group_by_clause
		if (groupBy != null) {
			result = new ProjectPlan(result, colName, alias, groupBy);
			if (havingCondition != null) {
				result = new SelectPlan(result, havingCondition);
			}
		} else {
			if (havingCondition != null) {
				throw new Exception("Wrong HAVING Condition: missing GROUP BY");
			}
			result = new ProjectPlan(result, colName, alias, null);
		}
		
		if (t.getType() == Symbol.SELECT_DISTINCT) {
			result = new DistinctPlan(result);
		}
		
		//order_by
		if (sortColumnList != null) {
			result = new SortPlan(result, sortColumnList, sortOrderList);
		}
		
		return result;
	}

	private Plan getCreateDatabase(CommonTree t) {
		String databaseName = t.getChild(0).toString();
		return new CreateDatabasePlan(databaseName);
	}

	private Plan getUseDatabase(CommonTree t) {
		String databaseName = t.getChild(0).toString();
		return new UseDatabasePlan(databaseName);
	}

	private Plan getDropDatabase(CommonTree t) {
		String databaseName = t.getChild(0).toString();
		return new DropDatabasePlan(databaseName);
	}

	private Plan getDelete(CommonTree t) throws Exception {
		String tableName = t.getChild(0).toString();
		Predicate whereCondition = null;
		if (t.getChildCount() == 2) {
			whereCondition = getCondition( t.getChild(1).getChild(0) );
		}
		return new DeletePlan(tableName, whereCondition);
	}

	private Plan getCreateIndex(CommonTree t) {
		String indexName = t.getChild(0).toString();
		String tableName = t.getChild(1).toString();
		String colName = translateColName(t.getChild(2));
		boolean isUnique = t.getType() == Symbol.CREATE_UNIQUE_INDEX;
		return new CreateIndexPlan(indexName, isUnique, tableName, colName);
	}

	private Plan getDropIndex(CommonTree t) {
		String indexName = t.getChild(0).toString();
		String tableName = t.getChild(1).toString();
		return new DropIndexPlan(indexName, tableName);
	}

	private Plan getInsertColumns(CommonTree t) throws Exception {
		String tableName = t.getChild(0).toString();
		List<String> schema = new LinkedList<String>();
		List<Predicate> values = null;
		for (int i = 1; i < t.getChildCount(); ++i) {
			if (t.getChild(i).getType() == Symbol.VALUES) {
				values = getValueTuple(t.getChild(i));
				break;
			}
			schema.add( translateColName(t.getChild(i)) );
		}
		return new InsertValuePlan(tableName, values, schema);
	}

	private Plan getInsertSubquery(CommonTree t) throws Exception {
		String tableName = t.getChild(0).toString();
		Plan subPlan = getSubQuery(t.getChild(1));
		return new InsertSubQueryPlan(tableName, subPlan);
	}

	private Plan getSubQuery(Tree child) throws Exception {// may need to change
		return new SubQueryPlan(getSelect((CommonTree)child));
	}

	private Plan getInsertValues(CommonTree t) throws Exception {
		String tableName = t.getChild(0).toString();
		List<Predicate> values = getValueTuple(t.getChild(1));
		return new InsertValuePlan(tableName, values, null);
	}

	private List<Predicate> getValueTuple(Tree child) throws Exception {
		List<Predicate> result = new LinkedList<Predicate>();
		for (int i = 0; i < child.getChildCount(); ++i) {
			System.out.println(child.getChild(i).toStringTree());
			result.add(translateValue(child.getChild(i)));
		}
		return result;
	}

	private Plan getCreateTable(CommonTree t) throws Exception {
		Schema schema = new Schema();
		List<String> primaryKeys = new ArrayList<String>();
		String tableName = t.getChild(0).toString();
		schema.setTableName(tableName);
		//System.err.println(t.getChildCount());
		for (int i = 1; i < t.getChildCount(); ++i) {//System.err.println(t.getChild(i).getType());
			Tree cur = t.getChild(i);
			Attribute att = new Attribute();
			if (cur.getType() == Symbol.CREATE_DEFINITION) {
				String colName = translateColName(cur.getChild(0));
				att.setColumnName(colName);
				
				DataType type = null;
				switch (cur.getChild(1).getType()) {
				case Symbol.INT:
					type = new IntegerType();
					break;
				case Symbol.FLOAT:
					type = new FloatType();
					break;
				case Symbol.CHAR:
					int length = Integer.parseInt( cur.getChild(1).getChild(0).toString() );
					type = new CharType(length);
					break;
				case Symbol.DATETIME:
					type = new DateTimeType();
					break;
				case Symbol.BOOLEAN:
					type = new BooleanType();
					break;
				case Symbol.DECIMAL:
					int precision = Integer.parseInt( cur.getChild(1).getChild(0).toString() );
					if (cur.getChild(1).getChildCount() == 1) {
						type = new DecimalType(precision, 0); 
					} else {
						int scale = Integer.parseInt( cur.getChild(1).getChild(1).toString() );
						type = new DecimalType(precision, scale);
					}
					break;
				case Symbol.TIMESTAMP:
					type = new TimestampType();
					break;
				case Symbol.VARCHAR:
					int maxLength = Integer.parseInt( cur.getChild(1).getChild(0).toString() );
					type = new VarcharType(maxLength);
					break;
				default:
					break;
				}
				att.setType(type);
				
				for (int j = 2; j < cur.getChildCount(); ++j) {
					Tree suffix = cur.getChild(j);
					if (suffix.getType() == Symbol.DEFAULT) {
						att.setDefault( getConstantValue(suffix.getChild(0)) );
					} else if (suffix.getType() == Symbol.AUTO_INCREMENT) {
						att.setAutoIncrement();
					} else if (suffix.getChildCount() == 0) {
						// null
						att.setNull(1);
					} else {
						// null not
						att.setNull(0);					
					}
				}
				
				//insert columns into schema
				schema.addColumn(att);
			} else if (cur.getType() == Symbol.PRIMARY_KEY) {
				primaryKeys.add(cur.getChild(0).toString());
			}
		}
		return new CreateTablePlan(schema, primaryKeys);
	}
	
	private Plan getDropTable(CommonTree t) {
		return new DropTablePlan(extractList(t.getChildren()));
	}

	private Plan getUpdate(CommonTree t) throws Exception {
		String tableName = t.getChild(0).toString();
		List<String> colNameList = new LinkedList<String>();
		List<Predicate> valueList = new LinkedList<Predicate>();
		Predicate whereCondition = null;
		for (int i = 1; i < t.getChildCount(); ++i) {
			if (t.getChild(i).getType() == Symbol.UPDATE_PAIR) {
				colNameList.add( translateColName( t.getChild(i).getChild(0) ) );
				valueList.add(translateValue( t.getChild(i).getChild(1) ));
			} else {
				whereCondition = getCondition( t.getChild(i).getChild(0) );
				break;
			}
		}
		return new UpdatePlan(tableName, colNameList, valueList, whereCondition);
	}

	private Predicate getCondition(Tree t) throws Exception {
		return translateOR(t);
	}

	private Predicate translatePrimary(Tree t) throws Exception {
		if (t.getType() == Symbol.NOT_EXISTS) {
			return new ExistsPredicate(true, getSubQuery(t.getChild(0)));
		} else if (t.getType() == Symbol.EXISTS) {
			return new ExistsPredicate(false, getSubQuery(t.getChild(0)));
		} else if (t.getType() == Symbol.IN) {
			return new InPredicate(translateValue(t.getChild(0)), 
					getSubQuery(t.getChild(1)));
		} else if (t.getType() == Symbol.ANY) {
			return new AnyPredicate(translateValue(t.getChild(0)),
					t.getChild(1).toString(),
					getSubQuery(t.getChild(2)));
		} else if (t.getType() == Symbol.ALL) {
			return new AllPredicate(translateValue(t.getChild(0)),
					t.getChild(1).toString(),
					getSubQuery(t.getChild(2)));			
		} else if (t.getChildCount() == 1) {//System.out.println(t.getType());
			// primary : bool_expr
			return translateOR(t);
		} else if (t.getType() == Symbol.LESS ||
				t.getType() == Symbol.LESS_EQ ||
				t.getType() == Symbol.GTR ||
				t.getType() == Symbol.GTR_EQ ||
				t.getType() == Symbol.EQ ||
				t.getType() == Symbol.NEQ) {
			// primary : value cop value
			return new BooleanCompPredicate(translateValue(t.getChild(0)),
					translateValue(t.getChild(1)),
					t.getType());
		} else {
			return translateOR(t);
		}
	}
	
	private Predicate translateOR(Tree t) throws Exception {
		if (t.getType() == Symbol.OR) {
			Predicate[] child = new Predicate[2];
			for (int i = 0; i < 2; ++i) {
				Tree tree = t.getChild(i);
				child[i] = translateOR(tree);		
			}
			return new BooleanPredicate(child[0], child[1], t.getType() );
		} else {
			return translateAND(t);
		}
	}

	private Predicate translateAND(Tree t) throws Exception {
		if (t.getType() == Symbol.AND) {
			Predicate[] child = new Predicate[2];
			for (int i = 0; i < 2; ++i) {
				Tree tree = t.getChild(i);
				child[i] = translateAND(tree);		
			}
			return new BooleanPredicate(child[0], child[1], t.getType() );
		} else {
			return translatePrimary(t);
		}
	}

	private Predicate translateValue(Tree t) throws Exception {
		if (t.getChildCount() == 2 && (t.getType() == Symbol.PLUS || t.getType() == Symbol.MINUS)) {
			return new NumberCalcPredicate(translateMultiplicative(t.getChild(0)),
					translateMultiplicative(t.getChild(1)),
					t.getType());
		} else {
			return translateMultiplicative(t);
		}			
	}
	
	private Predicate translateMultiplicative(Tree t) throws Exception {
		if (t.getChildCount() == 2 && (t.getType() == Symbol.MUL || t.getType() == Symbol.DIV || t.getType() == Symbol.MOD)) {
			return new NumberCalcPredicate(translateAtom(t.getChild(0)),
					translateAtom(t.getChild(1)),
					t.getType());
		} else {
			return translateAtom(t);
		}
	}

	private Predicate translateAtom(Tree child) throws Exception {//System.out.println(child.toStringTree());
		if (child.getType() == Symbol.DOT || child.getType() == Symbol.ID) {
			//col_name
			return new VariablePredicate(translateColName(child));// where to get the table_name?
		} else if (child.getType() == Symbol.SELECT || child.getType() == Symbol.SELECT_DISTINCT) {
			//subquery
			return new SubQueryPredicate(getSubQuery(child));
		} else if (child.getType() == Symbol.AVG ||
				child.getType() == Symbol.COUNT ||
				child.getType() == Symbol.MIN ||
				child.getType() == Symbol.MAX ||
				child.getType() == Symbol.SUM) {
			// func^ (! colName )!
			return new FuncPredicate(child.getType(), translateColName(child.getChild(0)));
		} else if (child.getType() == Symbol.MINUS) {
			// -^ atom
			return new NumberCalcPredicate(new ConstantPredicate(new IntegerData(0, new IntegerType())), translateAtom(child.getChild(0)), Symbol.MINUS);
		} else {
			// const_value
			Predicate result = null;
			try {
				result = getConstantValue(child);
			} catch (Exception ex) {
				// value
				return translateValue(child);
			}
			return result;
		}
	}

	private String translateColName(Tree child) {
		if (child.getType() == Symbol.DOT) {
			return child.getChild(0).toString() + "." + child.getChild(1).toString();
		} else {
			return child.toString();
		}
	}

	private Predicate getConstantValue(Tree child) throws Exception {
		if (child.getType() == Symbol.INTEGER_LITERAL) {
			if (child.toString().length() >= 10) {
				return new ConstantPredicate(new DecimalType(0, 0).valueOf(child.toString()));
			}
			return new ConstantPredicate(new IntegerType().valueOf(child.toString()));
		} else if (child.getType() == Symbol.STRING_LITERAL) {
			String c = child.toString();
			return new ConstantPredicate(new CharType(c.length()).valueOf(c));
		} else if (child.getType() == Symbol.FLOAT_LITERAL) {
			return new ConstantPredicate(new FloatType().valueOf(child.toString()));
		} else if (child.getType() == Symbol.TRUE) {
			return new ConstantPredicate(BooleanData.TRUE);
		} else if (child.getType() == Symbol.FALSE) {
			return new ConstantPredicate(BooleanData.FALSE);
		} else if (child.getType() == Symbol.NULL) {
			return new ConstantPredicate(null);
		} else if (child.getType() == Symbol.DEFAULT) {
			// ConstantPredicate is a null value => default value
			return null;
		}
		throw new Exception("Constant Format Error");
	}
}
