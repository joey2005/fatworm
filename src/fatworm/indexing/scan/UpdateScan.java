package fatworm.indexing.scan;

import java.util.*;

import fatworm.engine.predicate.Predicate;
import fatworm.indexing.LogicalFileMgr;
import fatworm.indexing.data.BooleanData;
import fatworm.indexing.data.Data;
import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;
import fatworm.util.Fatworm;

public class UpdateScan extends Operation {
	
	private Scan scan;
	private Schema schema;
	private List<String> colNameList;
	private List<Predicate> valueList;
	private Predicate whereCondition;
	
	public UpdateScan(Scan scan, Schema schema, List<String> colNameList, List<Predicate> valueList, Predicate whereCondition) {
		this.scan = scan;
		this.schema = schema;
		this.colNameList = colNameList;
		this.valueList = valueList;
		this.whereCondition = whereCondition;
	}

	@Override
	public void doit() {
		if (scan == null) {
			return;
		}

		int count = schema.getColumnCount();
		Data[] tmp = new Data[count];
		
		int pointer = 0;
		String tableName = schema.getTableName();
		
		scan.beforeFirst();
		while (scan.hasNext()) {
			Record record = scan.next();
			
			BooleanData ok = null;
			try {
				ok = whereCondition.test(record);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (!ok.isNull() && (Boolean)ok.getValue()) {
				for (int i = 0; i < count; ++i) {
					tmp[i] = record.getFromColumn(i);
				}
				for (int i = 0; i < colNameList.size(); ++i) {
					int pos = schema.indexOf(colNameList.get(i));
					tmp[pos] = valueList.get(i).calc(record);
				}
				ArrayList<Data> datas = new ArrayList<Data>();
				for (int i = 0; i < count; ++i) {
					datas.add(tmp[i]);
				}
				LogicalFileMgr.updateRecord(tableName, pointer, new Record(datas, schema));
			}
			pointer++;
		}
	}

	@Override
	public void close() {
		if (scan != null) {
			scan.close();
			scan = null;
		}
		colNameList = null;
		valueList = null;
		whereCondition = null;
	}

	@Override
	public String toString() {
		return "update table scan()";
	}

}
