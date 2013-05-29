package fatworm.engine.plan;

import fatworm.indexing.LogicalFileMgr;
import fatworm.indexing.data.*;
import fatworm.indexing.scan.InsertValueScan;
import fatworm.indexing.scan.Scan;
import fatworm.indexing.schema.*;
import fatworm.indexing.table.Record;
import fatworm.util.Fatworm;

import java.util.*;

import fatworm.engine.predicate.*;

public class InsertValuePlan extends Plan {
	
	public String tableName;
	public Schema schema;
	public ArrayList<Data> datas;
	public int planID;
	
	public InsertValuePlan(String tableName, List<Predicate> list, List<String> columns) {
		this.tableName = tableName;
		this.schema = LogicalFileMgr.getSchema(tableName);
		
		int count = schema.getColumnCount(), pos = 0;
		Data[] tmp = new Data[count];
		
		if (columns == null) {
			for (Predicate p : list) {
				if (p != null) {
					tmp[pos] = p.calc(null);
				} else {
					tmp[pos] = schema.getFromColumn(pos).getDefault();
				}
				pos++;
			}
		} else {
			for (String col : columns) {
				int dotpos = col.indexOf(".");
				String fieldName = tableName + "." + col.substring(dotpos + 1);
				int index = schema.indexOf(fieldName);
				if (list.get(pos) != null) {
					tmp[index] = list.get(pos).calc(null);
				} else {
					tmp[index] = schema.getFromColumn(index).getDefault();
				}
				pos++;
			}
		}
		for (int i = 0; i < count; ++i) {
			AttributeField af = schema.getFromColumn(i);
			if (af.autoIncrement) {
				tmp[i] = af.getAutoIncrement();
			}
			if (tmp[i] == null) {
				if (af.getDefault() != null) {
					tmp[i] = af.getDefault();
				} else {
					tmp[i] = af.getType().valueOf((String)null);
				}
			}
			if (af.isNull == af.ONLY_NOT_NULL && tmp[i].getValue() == null) {
				//ERROR
			}
			if (af.isNull == af.ONLY_NULL && tmp[i].getValue() != null) {
				//ERROR
			}
			if ((tmp[i] instanceof StringData) && !(tmp[i].getType().equals(af.getType()))) {
				tmp[i] = af.getType().valueOf(tmp[i].toString());
			}
		}
		
		datas = new ArrayList<Data>();
		for (int i = 0; i < count; ++i) {
			Data data = null;
			if (tmp[i] != null) {
				DataType datatype = tmp[i].getType();
				DataType realtype = schema.getFromColumn(i).getType();
				data = realtype.valueOf(tmp[i].toString());
			}
			datas.add(data);
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
