package fatworm.engine.plan;

import fatworm.indexing.data.Data;
import fatworm.indexing.scan.InsertValueScan;
import fatworm.indexing.scan.Scan;
import fatworm.indexing.schema.AttributeField;
import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;
import fatworm.util.Fatworm;

import java.util.ArrayList;
import java.util.List;

import fatworm.engine.predicate.*;

public class InsertValuePlan extends Plan {
	
	public String tableName;
	public Schema schema;
	public ArrayList<Data> datas;
	public int planID;
	
	public InsertValuePlan(String tableName, List<Predicate> list, List<String> columns) {
		this.tableName = tableName;
		this.schema = Fatworm.tx.infoMgr.getSchema(tableName);
		this.datas = new ArrayList<Data>();
		
		if (columns == null) {
			for (Predicate p : list) {
				datas.add(p.calc(null));
			}
		} else {
			int count = schema.getColumnCount(), pos = 0;
			Data[] tmp = new Data[count];
			for (String col : columns) {
				int dotpos = col.indexOf(".");
				String fieldName = tableName + "." + col.substring(dotpos + 1);
				int index = schema.indexOf(fieldName);
				tmp[index] = list.get(pos++).calc(null);
			}
			for (int i = 0; i < count; ++i) {
				AttributeField af = schema.getFromColumn(i);
				if (af.autoIncrement) {
					tmp[i] = af.getAutoIncrement();
				}
				if (af.defaultValue != null) {
					tmp[i] = af.getDefault();
				}
			}
			for (int i = 0; i < count; ++i) {
				datas.add(tmp[i]);
			}
		}
	}
	
	@Override
	public String toString() {
		String result = "insert into " + tableName + ": value( ";
		for (Data data : datas) {
			result += data.toString() + ", ";
		}
		result += " )";
		return "Plan #" + (planID = Plan.planCount++) + " <- " + result;
	}

	@Override
	public int getPlanID() {
		return planID;
	}

	@Override
	public Scan createScan() {
		return new InsertValueScan(tableName, new Record(datas, schema));
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
