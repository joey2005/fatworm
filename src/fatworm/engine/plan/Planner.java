package fatworm.engine.plan;

import fatworm.indexing.LogicalFileMgr;
import fatworm.indexing.data.*;
import fatworm.indexing.schema.*;
import fatworm.util.Fatworm;

import java.util.*;

import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;

import fatworm.engine.predicate.*;
import fatworm.engine.symbol.Symbol;

public class Planner {
	
	Map<String, String> AliasToTblname;
	Map<String, Schema> AliasToSchema;
	Map<String, String> AliasToColname;
	Map<String, DataType> ColnameToType;
	CommonTree tree;
	List<String> AllTableNameList;
	
	/**
	 * return the schema of given table
	 * @param tableName
	 * @return
	 */
	private Schema getSchema(String tableName) {
		if (AliasToSchema.containsKey(tableName)) {
			return AliasToSchema.get(tableName);
		}
		return LogicalFileMgr.getSchema(tableName);
	}

	/**
	 * return then data type of give column name (may not have table name)
	 * @param colName
	 * @return
	 * @throws Exception 
	 */
	private DataType getType(String colName) throws Exception {
		int dotpos = colName.indexOf(".");
		String tableName = colName.substring(0, dotpos);
		
		DataType result = null;
		
		if (tableName.equals("")) {
			String fieldName = colName.substring(dotpos + 1);
			if (ColnameToType.containsKey(fieldName)) {
				result = ColnameToType.get(fieldName);
			}
			if (result == null) {
				for (String name : AllTableNameList) {
					String variableName = name + "." + fieldName;
					try {
						Schema s = LogicalFileMgr.getSchema(name);
						result = s.getFromVariableName(variableName).getType();
					} catch (Exception ex) {
						result = null;
					}
					if (result != null) {
						break;
					}
				}
			}
		}
		
		if (result == null) {
			if (AliasToSchema.containsKey(tableName)) {
				Schema s = AliasToSchema.get(tableName);
				result = s.getFromVariableName(colName).getType();
			}
		}
		
		if (result == null) {
		
			if (AliasToTblname.containsKey(tableName)) {
				tableName = AliasToTblname.get(tableName);
			}
		
			Schema s = LogicalFileMgr.getSchema(tableName);
			
			if (AliasToColname.containsKey(colName)) {
				colName = AliasToColname.get(colName);
			}
			dotpos = colName.indexOf(".");
			colName = tableName + "." + colName.substring(dotpos + 1);
			result = s.getFromVariableName(colName).getType();
		}
		
		if (result == null) {
			throw new Exception("cannot find type");
		}
		
		return result;
	}
	
	public Planner(CommonTree tree) {
		this.tree = tree;
		this.AliasToTblname = new HashMap<String, String>();
		this.AliasToSchema = new HashMap<String, Schema>();
		this.ColnameToType = new HashMap<String, DataType>();
		this.AliasToColname = new HashMap<String, String>();
		this.AllTableNameList = new ArrayList<String>();
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

	private Plan getSelect(CommonTree t) throws Exception {
		// work with select_expr
		Plan result = null;
		
		boolean selectAll = false;
		
		List<Tree> selectExprList = new ArrayList<Tree>();
		List<String> newAlias = new ArrayList<String>();
		
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
			} else if (cur.getType() == Symbol.MUL && cur.getChildCount() == 0) {
				// '*'
				selectAll = true;
			} else if (cur.getType() == Symbol.AS) {
				// value (AS^ alias)
				selectExprList.add(cur.getChild(0));
				newAlias.add(cur.getChild(1).toString().toLowerCase());
			} else {
				selectExprList.add(cur);
				newAlias.add(null);
			}
				
			++i;
		}
		
		if (selectAll && selectExprList.size() > 0) {
			throw new Exception("Select Content Wrong!");
		}
		
		List<Plan> tableRefList = new ArrayList<Plan>();
		Tree whereConditionTree = null;
		String groupBy = null;
		Tree havingConditionTree = null;
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
					
					if (cur.getType() == Symbol.AS) {
						if (cur.getChild(0).getType() == Symbol.SELECT || cur.getChild(0).getType() == Symbol.SELECT_DISTINCT) {
							// subquery as alias
							tableRefList.add(new RenamePlan(getSubQuery(cur.getChild(0)), cur.getChild(1).toString().toLowerCase()));
						} else {
							String tableName = cur.getChild(0).toString().toLowerCase();
							tableRefList.add(new RenamePlan(new TablePlan(tableName, getSchema(tableName)), cur.getChild(1).toString().toLowerCase()));
							AllTableNameList.add(tableName);
						}
					} else {
						String tableName = cur.toString().toLowerCase();
						tableRefList.add(new TablePlan(tableName, getSchema(tableName)));
						AllTableNameList.add(tableName);
					}
				}
			} else if (child.getType() == Symbol.WHERE) {
				whereConditionTree = child.getChild(0);
			} else if (child.getType() == Symbol.GROUP) {
				groupBy = translateColName("", child.getChild(0));
			} else if (child.getType() == Symbol.HAVING) {
				havingConditionTree = child.getChild(0);
			} else if (child.getType() == Symbol.ORDER) {
				if (sortColumnList == null) {
					sortColumnList = new LinkedList<String>();
					sortOrderList = new LinkedList<Boolean>();
					for (int pos = 0; pos < child.getChildCount(); ++pos) {
						Tree cur = child.getChild(pos);
						
						if (cur.getType() == Symbol.ASC) {
							sortColumnList.add( translateColName("", cur.getChild(0)) );
							sortOrderList.add( Boolean.TRUE );							
						} else if (cur.getType() == Symbol.DESC) {
							sortColumnList.add( translateColName("", cur.getChild(0)) );
							sortOrderList.add( Boolean.FALSE );								
						} else {
							sortColumnList.add( translateColName("", cur) );
							sortOrderList.add( Boolean.TRUE );
						}
					}
				}
			}
			
			++i;
		}
		
		//==================================translate plan begins==================================
		//table_ref
		result = translateTableRef(tableRefList);
		
		List<Predicate> projectList = new ArrayList<Predicate>();
		for (Tree tree : selectExprList) {
			projectList.add(translateValue(tree, ""));
		}
		
		//where_condition
		Predicate whereCondition = null;
		if (whereConditionTree != null) { 
			whereCondition = getCondition(whereConditionTree, "");
		}
		
		//whereCondition = replaceAlias(whereCondition);
		
		result = new SelectPlan(result, whereCondition);
		
		//group_by_clause
		boolean changed = false;
		Set<FuncPredicate> allfs = new HashSet<FuncPredicate>();
		for (Predicate p : projectList) {
			allfs.addAll(getAllFunc(p));
		}
		List<Predicate> projectList2 = new ArrayList<Predicate>(projectList);
		Predicate havingCondition = null;
		if (groupBy != null) {
			if (havingConditionTree != null) {
				havingCondition = getCondition(havingConditionTree, "");
			}
			if (havingCondition != null) {
				allfs.addAll(getAllFunc(havingCondition));
			}
		}
				
		for (FuncPredicate fp : allfs) {
			boolean contained = false;
			for (Predicate p : projectList2) {
				if (p.toString().equals(fp.toString())) {
					contained = true;
					break;
				}
			}
			if (!contained) {
				projectList2.add(fp);
				changed = true;
			}
		}
				
		if (changed) {
			result = new ProjectPlan(result, projectList2, groupBy);
			if (havingCondition != null) {
				Schema schema = result.getSchema();
				Predicate predict = translateHavingCondition(havingCondition, schema.getTableName());
				result = new SelectPlan(result, predict);
			}
			
			List<Predicate> projectList3 = new ArrayList<Predicate>();
			String tableName = result.getSchema().getTableName();
			for (Predicate p : projectList) {
				if (p instanceof ConstantPredicate) {
					ConstantPredicate cp = (ConstantPredicate) p;
					projectList3.add(new VariablePredicate(tableName + "." + cp.toString(), cp.getType()));
				} else if (p instanceof FuncPredicate) {
					FuncPredicate fp = (FuncPredicate) p;
					projectList3.add(new VariablePredicate(tableName + "." + fp.toString(), fp.getType()));
				} else if (p instanceof NumberCalcPredicate) {
					NumberCalcPredicate np = (NumberCalcPredicate) p;
					projectList3.add(new VariablePredicate(tableName + "." + np.toString(), np.getType()));
				} else if (p instanceof VariablePredicate) {
					VariablePredicate vp = (VariablePredicate) p;
					projectList3.add(new VariablePredicate(tableName + "." + vp.toString(), vp.getType()));
				}
			}
			//order_by
			if (sortColumnList != null) {
				result = new SortPlan(result, sortColumnList, sortOrderList);
			}
			result = new ProjectPlan(result, projectList3, null);
		} else {
			if (groupBy != null) {
				//order_by
				if (sortColumnList != null) {
					result = new SortPlan(result, sortColumnList, sortOrderList);
				}
				result = new ProjectPlan(result, projectList, groupBy);
				if (havingCondition != null) {
					Schema schema = result.getSchema();
					Predicate predict = translateHavingCondition(havingCondition, schema.getTableName());
					result = new SelectPlan(result, predict);					
				}
			} else {
				if (!selectAll) {
					//order_by
					if (sortColumnList != null) {
						result = new SortPlan(result, sortColumnList, sortOrderList);
					}
					result = new ProjectPlan(result, projectList, null);
				} else {
					//order_by
					if (sortColumnList != null) {
						result = new SortPlan(result, sortColumnList, sortOrderList);
					}
				}
			}
		}
		
		Schema s2 = translateSelectExpr(projectList, newAlias, result.getSchema());
		if (s2 != null) {
			result = new RenamePlan(result, s2);
		}
		
		if (t.getType() == Symbol.SELECT_DISTINCT) {
			result = new DistinctPlan(result);
		}
		
		return result;
	}
	
	private Predicate replaceAlias(Predicate p) {
		if (p instanceof AllPredicate) {
			AllPredicate ap = (AllPredicate) p;
			Predicate result = replaceAlias(ap.value);
			return new AllPredicate(result, ap.oper, ap.subPlan);
		} else if (p instanceof AnyPredicate) {
			AnyPredicate ap = (AnyPredicate) p;
			Predicate result = replaceAlias(ap.value);
			return new AnyPredicate(result, ap.oper, ap.subPlan);
		} else if (p instanceof BooleanCompPredicate) {
			BooleanCompPredicate bcp = (BooleanCompPredicate) p;
			Predicate left = replaceAlias(bcp.lhs);
			Predicate right = replaceAlias(bcp.rhs);
			return new BooleanCompPredicate(left, right, bcp.oper);
		} else if (p instanceof BooleanPredicate) {
			BooleanPredicate bp = (BooleanPredicate) p;
			Predicate left = replaceAlias(bp.lhs);
			Predicate right = replaceAlias(bp.rhs);
			return new BooleanPredicate(left, right, bp.oper);
		} else if (p instanceof FuncPredicate) {
			FuncPredicate fp = (FuncPredicate) p;
			Predicate result = replaceAlias(fp.colName);
			return new FuncPredicate(fp.func, (VariablePredicate) result);
		} else if (p instanceof InPredicate) {
			InPredicate ip = (InPredicate) p;
			Predicate result = replaceAlias(ip.value);
			return new InPredicate(result, ip.subPlan);
		} else if (p instanceof NumberCalcPredicate) {
			NumberCalcPredicate ncp = (NumberCalcPredicate) p;
			Predicate left = replaceAlias(ncp.lhs);
			Predicate right = replaceAlias(ncp.rhs);
			return new NumberCalcPredicate(left, right, ncp.oper, ncp.getType());
		} else if (p instanceof VariablePredicate) {
			VariablePredicate vp = (VariablePredicate) p;
			String colname = vp.variableName;
			int dotpos = colname.indexOf(".");
			if (dotpos > 0) {
				String tablename = colname.substring(0, dotpos);
				String fieldname = colname.substring(dotpos + 1);
				String originTablename = AliasToTblname.get(tablename);
				if (originTablename != null) {
					colname = originTablename + "." + fieldname;
				}
				return new VariablePredicate(colname, vp.getType());
			}
		}
		return p;
	}

	private Schema translateSelectExpr(List<Predicate> projectList,
			List<String> newAlias, Schema schema) {
		List<AttributeField> attrList = new ArrayList<AttributeField>();
		boolean changed = false;
		for (int i = 0; i < projectList.size(); ++i) {
			AttributeField af = schema.getFromColumn(i);
			if (newAlias.get(i) != null) {
				changed = true;
				
				String fieldName = af.colName;
				int dotpos = fieldName.indexOf(".");
				String tableName = fieldName.substring(0, dotpos);
				fieldName = tableName + "." + newAlias.get(i);
				attrList.add(new AttributeField(fieldName, af.type, af.isNull, af.defaultValue, af.autoIncrement));
			
				ColnameToType.put(newAlias.get(i), af.getType());
				AliasToColname.put(tableName + "." + newAlias.get(i), af.getColumnName());
			} else {
				attrList.add(af);
			}
		}
		if (!changed) {
			return null;
		} else {
			return new Schema(schema.getTableName(), attrList);
		}
	}

	private Predicate translateHavingCondition(Predicate p,
			String tableName) {
		if (p instanceof AllPredicate) {
			return new AllPredicate(translateHavingCondition(((AllPredicate) p).value, tableName), 
					((AllPredicate) p).oper,
					((AllPredicate) p).subPlan);
		} else if (p instanceof AnyPredicate) {
			return new AnyPredicate(translateHavingCondition(((AnyPredicate) p).value, tableName),
					((AnyPredicate) p).oper,
					((AnyPredicate) p).subPlan);
		} else if (p instanceof BooleanCompPredicate) {
			return new BooleanCompPredicate(translateHavingCondition(((BooleanCompPredicate) p).lhs, tableName),
					translateHavingCondition(((BooleanCompPredicate) p).rhs, tableName),
					((BooleanCompPredicate) p).oper);
		} else if (p instanceof BooleanPredicate) {
			return new BooleanPredicate(translateHavingCondition(((BooleanPredicate) p).lhs, tableName), 
					translateHavingCondition(((BooleanPredicate) p).rhs, tableName),
					((BooleanPredicate) p).oper);
		} else if (p instanceof FuncPredicate) {
			return new VariablePredicate(tableName + "." + p.toString(), p.getType());
		} else if (p instanceof InPredicate) {
			return new InPredicate(translateHavingCondition(((InPredicate) p).value, tableName), 
					((InPredicate) p).subPlan);
		} else if (p instanceof NumberCalcPredicate) {
			NumberCalcPredicate ncp = (NumberCalcPredicate) p;
			return new NumberCalcPredicate(translateHavingCondition(ncp.lhs, tableName), 
					translateHavingCondition(ncp.rhs, tableName), 
					ncp.oper,
					ncp.getType());
		}
		return p;
	}


	private Set<FuncPredicate> getAllFunc(Predicate p) {
		Set<FuncPredicate> result = new HashSet<FuncPredicate>();
		if (p instanceof AllPredicate) {
			result.addAll(getAllFunc(((AllPredicate) p).value));
		} else if (p instanceof AnyPredicate) {
			result.addAll(getAllFunc(((AnyPredicate) p).value));
		} else if (p instanceof BooleanCompPredicate) {
			result.addAll(getAllFunc(((BooleanCompPredicate) p).lhs));
			result.addAll(getAllFunc(((BooleanCompPredicate) p).rhs));
		} else if (p instanceof BooleanPredicate) {
			result.addAll(getAllFunc(((BooleanPredicate) p).lhs));
			result.addAll(getAllFunc(((BooleanPredicate) p).rhs));
		} else if (p instanceof FuncPredicate) {
			result.add((FuncPredicate)p);
		} else if (p instanceof InPredicate) {
			result.addAll(getAllFunc(((InPredicate) p).value));
		} else if (p instanceof NumberCalcPredicate) {
			result.addAll(getAllFunc(((NumberCalcPredicate) p).lhs));
			result.addAll(getAllFunc(((NumberCalcPredicate) p).rhs));
		}
		return result;
	}


	private Plan translateTableRef(List<Plan> tableRefList) {
		if (tableRefList == null || tableRefList.isEmpty()) {
			return new TablePlan("fakeTable", getSchema("fakeTable"));
		}
		Plan result = null;
		for (Plan tableRef : tableRefList) {
			if (tableRef instanceof RenamePlan) {
				RenamePlan rplan = (RenamePlan) tableRef;
				Plan p = rplan.subPlan();
				String alias = rplan.alias;
				if (p instanceof TablePlan) {
					TablePlan tplan = (TablePlan) p;
					if (alias != null) {
						AliasToTblname.put(alias, tplan.tableName);
					}
				} else if (p instanceof SubQueryPlan) {
					if (alias != null) {
						AliasToSchema.put(alias, p.getSchema());
					}
				}
			}
			if (result == null) {
				result = tableRef;
			} else {
				result = new ProductPlan(result, tableRef);
			}
		}
		return result;
	}


	private Plan getCreateDatabase(CommonTree t) {
		String databaseName = t.getChild(0).toString().toLowerCase();
		return new CreateDatabasePlan(databaseName);
	}

	private Plan getUseDatabase(CommonTree t) {
		String databaseName = t.getChild(0).toString().toLowerCase();
		return new UseDatabasePlan(databaseName);
	}

	private Plan getDropDatabase(CommonTree t) {
		String databaseName = t.getChild(0).toString().toLowerCase();
		return new DropDatabasePlan(databaseName);
	}

	private Plan getDelete(CommonTree t) throws Exception {
		String tableName = t.getChild(0).toString().toLowerCase();
		Predicate whereCondition = null;
		if (t.getChildCount() == 2) {
			whereCondition = getCondition(t.getChild(1).getChild(0), tableName);
		}
		return new DeletePlan(tableName, whereCondition);
	}

	private Plan getCreateIndex(CommonTree t) {
		String indexName = t.getChild(0).toString().toLowerCase();
		String tableName = t.getChild(1).toString().toLowerCase();
		String colName = translateColName("", t.getChild(2));
		boolean isUnique = t.getType() == Symbol.CREATE_UNIQUE_INDEX;
		return new CreateIndexPlan(indexName, isUnique, tableName, colName);
	}

	private Plan getDropIndex(CommonTree t) {
		String indexName = t.getChild(0).toString().toLowerCase();
		String tableName = t.getChild(1).toString().toLowerCase();
		return new DropIndexPlan(indexName, tableName);
	}

	private Plan getInsertColumns(CommonTree t) throws Exception {
		String tableName = t.getChild(0).toString().toLowerCase();
		List<String> schema = new LinkedList<String>();
		List<Predicate> values = null;
		for (int i = 1; i < t.getChildCount(); ++i) {
			if (t.getChild(i).getType() == Symbol.VALUES) {
				values = getValueTuple(t.getChild(i), tableName);
				break;
			}
			schema.add(translateColName(tableName, t.getChild(i)));
		}
		return new InsertValuePlan(tableName, values, schema);
	}

	private Plan getInsertSubquery(CommonTree t) throws Exception {
		String tableName = t.getChild(0).toString().toLowerCase();
		Plan subPlan = getSubQuery(t.getChild(1));
		return new InsertSubQueryPlan(tableName, subPlan);
	}

	private Plan getSubQuery(Tree child) throws Exception {// may need to change
		//return new SubQueryPlan(getSelect((CommonTree)child));
		return getSelect((CommonTree)child);
	}

	private Plan getInsertValues(CommonTree t) throws Exception {
		String tableName = t.getChild(0).toString().toLowerCase();
		List<Predicate> values = getValueTuple(t.getChild(1), tableName);
		return new InsertValuePlan(tableName, values, null);
	}

	private List<Predicate> getValueTuple(Tree child, String tableName) throws Exception {
		List<Predicate> result = new ArrayList<Predicate>();
		for (int i = 0; i < child.getChildCount(); ++i) {
			result.add(translateValue(child.getChild(i), tableName));
		}
		return result;
	}

	private Plan getCreateTable(CommonTree t) throws Exception {
		List<String> primaryKeys = new ArrayList<String>();
		List<AttributeField> attrList = new ArrayList<AttributeField>();
		
		String tableName = t.getChild(0).toString().toLowerCase();
		//System.err.println(t.getChildCount());
		for (int i = 1; i < t.getChildCount(); ++i) {//System.err.println(t.getChild(i).getType());
			Tree cur = t.getChild(i);
			if (cur.getType() == Symbol.CREATE_DEFINITION) {
				String colName = translateColName(tableName, cur.getChild(0));
	
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
				
				int isNull = -1;
				Predicate defaultValue = null;
				boolean autoIncrement = false;
				
				for (int j = 2; j < cur.getChildCount(); ++j) {
					Tree suffix = cur.getChild(j);
					if (suffix.getType() == Symbol.DEFAULT) {
						defaultValue = getConstantValue(suffix.getChild(0));
					} else if (suffix.getType() == Symbol.AUTO_INCREMENT) {
						autoIncrement = true;
					} else if (suffix.getChildCount() == 0) {
						// null
						isNull = AttributeField.ONLY_NULL;
					} else {
						// null not
						isNull = AttributeField.ONLY_NOT_NULL;					
					}
				}
				
				//insert columns into schema
				attrList.add(new AttributeField(colName, type, isNull, defaultValue, autoIncrement));
			} else if (cur.getType() == Symbol.PRIMARY_KEY) {
				primaryKeys.add(cur.getChild(0).toString().toLowerCase());
			}
		}
		return new CreateTablePlan(new Schema(tableName, attrList), primaryKeys);
	}
	
	private Plan getDropTable(CommonTree t) {
		List<String> tableNameList = new ArrayList<String>();
		for (int i = 0; i < t.getChildCount(); ++i) {
			tableNameList.add(t.getChild(i).toString().toLowerCase());
		}
		return new DropTablePlan(tableNameList);
	}

	private Plan getUpdate(CommonTree t) throws Exception {
		String tableName = t.getChild(0).toString().toLowerCase();
		List<String> colNameList = new ArrayList<String>();
		List<Predicate> valueList = new ArrayList<Predicate>();
		Predicate whereCondition = null;
		for (int i = 1; i < t.getChildCount(); ++i) {
			if (t.getChild(i).getType() == Symbol.UPDATE_PAIR) {
				colNameList.add(translateColName(tableName, t.getChild(i).getChild(0)));
				valueList.add(translateValue(t.getChild(i).getChild(1), tableName));
			} else {
				whereCondition = getCondition(t.getChild(i).getChild(0), tableName);
				break;
			}
		}
		return new UpdatePlan(tableName, colNameList, valueList, whereCondition);
	}

	private Predicate getCondition(Tree t, String tableName) throws Exception {
		return translateOR(t, tableName);
	}

	private Predicate translatePrimary(Tree t, String tableName) throws Exception {
		if (t.getType() == Symbol.NOT_EXISTS) {
			return new ExistsPredicate(true, getSubQuery(t.getChild(0)));
		} else if (t.getType() == Symbol.EXISTS) {
			return new ExistsPredicate(false, getSubQuery(t.getChild(0)));
		} else if (t.getType() == Symbol.IN) {
			return new InPredicate(translateValue(t.getChild(0), tableName), 
					getSubQuery(t.getChild(1)));
		} else if (t.getType() == Symbol.ANY) {
			return new AnyPredicate(translateValue(t.getChild(0), tableName),
					t.getChild(1).toString(),
					getSubQuery(t.getChild(2)));
		} else if (t.getType() == Symbol.ALL) {
			return new AllPredicate(translateValue(t.getChild(0), tableName),
					t.getChild(1).toString(),
					getSubQuery(t.getChild(2)));			
		} else if (t.getChildCount() == 1) {
			// primary : bool_expr
			return translateOR(t, tableName);
		} else if (t.getType() == Symbol.LESS ||
				t.getType() == Symbol.LESS_EQ ||
				t.getType() == Symbol.GTR ||
				t.getType() == Symbol.GTR_EQ ||
				t.getType() == Symbol.EQ ||
				t.getType() == Symbol.NEQ) {
			// primary : value cop value
			return new BooleanCompPredicate(translateValue(t.getChild(0), tableName),
					translateValue(t.getChild(1), tableName),
					t.getType());
		} else {
			return translateOR(t, tableName);
		}
	}
	
	private Predicate translateOR(Tree t, String tableName) throws Exception {
		if (t.getType() == Symbol.OR) {
			Predicate[] child = new Predicate[2];
			for (int i = 0; i < 2; ++i) {
				Tree tree = t.getChild(i);
				child[i] = translateOR(tree, tableName);		
			}
			return new BooleanPredicate(child[0], child[1], t.getType() );
		} else {
			return translateAND(t, tableName);
		}
	}

	private Predicate translateAND(Tree t, String tableName) throws Exception {
		if (t.getType() == Symbol.AND) {
			Predicate[] child = new Predicate[2];
			for (int i = 0; i < 2; ++i) {
				Tree tree = t.getChild(i);
				child[i] = translateAND(tree, tableName);		
			}
			return new BooleanPredicate(child[0], child[1], t.getType() );
		} else {
			return translatePrimary(t, tableName);
		}
	}

	private Predicate translateValue(Tree t, String tableName) throws Exception {
		if (t.getChildCount() == 2 && (t.getType() == Symbol.PLUS || t.getType() == Symbol.MINUS)) {
			Predicate left = translateMultiplicative(t.getChild(0), tableName);
			Predicate right = translateMultiplicative(t.getChild(1), tableName);
			int oper = t.getType();
			NumberType type = (NumberType)getOpType(left, right, oper);
			return new NumberCalcPredicate(left, right, oper, type);
		} else {
			return translateMultiplicative(t, tableName);
		}			
	}
	
	private Predicate translateMultiplicative(Tree t, String tableName) throws Exception {
		if (t.getChildCount() == 2 && (t.getType() == Symbol.MUL || t.getType() == Symbol.DIV || t.getType() == Symbol.MOD)) {
			Predicate left = translateAtom(t.getChild(0), tableName);
			Predicate right = translateAtom(t.getChild(1), tableName);
			int oper = t.getType();
			NumberType type = (NumberType)getOpType(left, right, oper);
			return new NumberCalcPredicate(left, right, oper, type);
		} else {
			return translateAtom(t, tableName);
		}
	}

	private Predicate translateAtom(Tree child, String tableName) throws Exception {
		if (child.getType() == Symbol.DOT || child.getType() == Symbol.ID) {
			//col_name
			String colName = translateColName(tableName, child);
			return new VariablePredicate(colName, getType(colName));// where to get the table_name?
		} else if (child.getType() == Symbol.SELECT || child.getType() == Symbol.SELECT_DISTINCT) {
			//subquery
			return new SubQueryPredicate(getSubQuery(child));
		} else if (child.getType() == Symbol.AVG ||
				child.getType() == Symbol.COUNT ||
				child.getType() == Symbol.MIN ||
				child.getType() == Symbol.MAX ||
				child.getType() == Symbol.SUM) {
			// func^ (! colName )!
			String colName = translateColName(tableName, child.getChild(0));
			DataType type = getType(colName);
			return new FuncPredicate(child.getType(), new VariablePredicate(colName, type));
		} else if (child.getType() == Symbol.MINUS && child.getChildCount() == 1) {
			// -^ atom
			Predicate left = new ConstantPredicate(new IntegerData(0, new IntegerType()));
			Predicate right = translateAtom(child.getChild(0), tableName);
			int oper = Symbol.MINUS;
			NumberType type = (NumberType)getOpType(left, right, oper);
			return new NumberCalcPredicate(left, right, oper, type);
		} else {
			// const_value
			Predicate result = null;
			try {
				result = getConstantValue(child);
			} catch (Exception ex) {
				// value
				return translateValue(child, tableName);
			}
			return result;
		}
	}

	private String translateColName(String tableName, Tree child) {
		if (child.getType() == Symbol.DOT) {
			return child.getChild(0).toString().toLowerCase() + "." + child.getChild(1).toString().toLowerCase();
		} else if (tableName != null && tableName.length() > 0) {
			return tableName + "." + child.toString().toLowerCase();
		} else {
			return "." + child.toString().toLowerCase();
		}
	}

	private Predicate getConstantValue(Tree child) throws Exception {
		if (child.getType() == Symbol.INTEGER_LITERAL) {
			String number = child.toString();
			Integer i = null;
			try {
				i = Integer.parseInt(number);
				if (!(i.toString().equals(number))) {
					throw new RuntimeException();
				}
				return new ConstantPredicate(new IntegerData(i, new IntegerType()));
			} catch (Exception ex) {
				return new ConstantPredicate(new DecimalType(number.length(), 0).valueOf(number));
			}
		} else if (child.getType() == Symbol.STRING_LITERAL) {
			String c = child.toString();
			if (c.length() >= 2 && c.charAt(0) == '\'' && c.charAt(c.length() - 1) == '\'') {
				c = c.substring(1, c.length() - 1);
			}
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

	private DataType getOpType(Predicate p1, Predicate p2, int op) throws Exception {
		DataType t1 = p1.getType();
		DataType t2 = p2.getType();
		DataType result = null;
		if (t1 instanceof IntegerType) {
			if (t2 instanceof IntegerType) {
				if (op == Symbol.DIV) {
					result = new FloatType();
				} else {
					result = new IntegerType();
				}
			} else if (t2 instanceof FloatType) {
				result = new FloatType();
			} else if (t2 instanceof DecimalType) {
				result = new DecimalType(((DecimalType)t2).getPrecision(), 
						((DecimalType)t2).getScale());
			}
		} else if (t1 instanceof CharType) {
			
		} else if (t1 instanceof FloatType) {
			if (t2 instanceof IntegerType) {
				result = new FloatType();
			} else if (t2 instanceof FloatType) {
				result = new FloatType();
			} else if (t2 instanceof DecimalType) {
				result = new DecimalType(((DecimalType)t2).getPrecision(), 
						((DecimalType)t2).getScale());	
			}
		} else if (t1 instanceof DecimalType) {
			if (t2 instanceof IntegerType) {
				result = new DecimalType(((DecimalType)t1).getPrecision(), 
						((DecimalType)t1).getScale());				
			} else if (t2 instanceof FloatType) {
				result = new DecimalType(((DecimalType)t1).getPrecision(), 
						((DecimalType)t1).getScale());
			} else if (t2 instanceof DecimalType) {
				int precision1 = ((DecimalType)t1).getPrecision();
				int scale1 = ((DecimalType)t1).getScale();
				int precision2 = ((DecimalType)t2).getPrecision();
				int scale2 = ((DecimalType)t2).getScale();
				int scale = Math.max(scale1, scale2);
				int precision = precision1 - scale1 > precision2 - scale2 ?
						precision1 - scale1 + scale : precision2 - scale2 + scale;
				result = new DecimalType(precision, scale);
			}
		}
		if (op == Symbol.MOD) {
			if (!(t2 instanceof IntegerType)) {
				result = null;
			}
		}
		if (result == null) {
			throw new Exception("getOpType");
		}
		return result;
	}
}
