package fatworm.indexing.scan;

import java.util.*;

import fatworm.engine.predicate.Predicate;
import fatworm.indexing.LogicalFileMgr;
import fatworm.indexing.data.BooleanData;
import fatworm.indexing.data.Data;
import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;
import fatworm.indexing.table.TableFile;
import fatworm.util.Fatworm;

public class UpdateScan extends Operation {
	
	private TableFile tf;
	private Schema schema;
	private List<String> colNameList;
	private List<Predicate> valueList;
	private Predicate whereCondition;
	
	public UpdateScan(String tableName, Schema schema, List<String> colNameList, List<Predicate> valueList, Predicate whereCondition) {
		this.tf = Fatworm.metadataMgr().getTableAccess(tableName);
		this.schema = schema;
		this.colNameList = colNameList;
		this.valueList = valueList;
		this.whereCondition = whereCondition;
	}

	@Override
	public void doit() {

		int count = schema.getColumnCount();
		Data[] tmp = new Data[count];
		
		tf.beforeFirst();
		while (tf.hasNext()) {
			Record record = tf.next();
			
			BooleanData ok = null;
			try {
				ok = whereCondition.test(record);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (!ok.isNull() && (Boolean)ok.getValue()) {
				Record result = record;
				int ptr = 0;
				for (Predicate p : valueList) {
					int pos = schema.indexOf(colNameList.get(ptr));
					Data value = p.calc(result);
					result.setValue(pos, value);
					tf.updateRecord(result);
					ptr++;
				}
			}
		}
		
		tf.close();
	}

	@Override
	public void close() {
		colNameList = null;
		valueList = null;
		whereCondition = null;
	}

	@Override
	public String toString() {
		return "update table scan()";
	}

}
