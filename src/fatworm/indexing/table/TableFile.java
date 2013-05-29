package fatworm.indexing.table;

import java.math.BigInteger;
import java.util.*;

import fatworm.indexing.data.*;
import fatworm.indexing.metadata.TableInfo;
import fatworm.indexing.schema.AttributeField;
import fatworm.indexing.schema.Schema;
import fatworm.util.Lib;

public class TableFile {

	private TableInfo ti;
	private RecordFile rf;
	
	public TableFile(TableInfo ti) {
		this.ti = ti;
		this.rf = new RecordFile(ti);
	}
	
	public Record getRecordFromRid(RID rid) {
		List<Data> datas = new ArrayList<Data>();
		Schema s = ti.schema();
		rf.moveToRid(rid);
		for (AttributeField af : s.getAllFields()) {
			DataType type = af.getType();
			String fieldName = af.getColumnName();
			String val = rf.getString(fieldName);
			if (type instanceof IntegerType) {
				IntegerType itype = (IntegerType) type;
				if (val.equals("null")) {
					datas.add(new IntegerData((String)null, itype));
				} else {
					int i = Integer.parseInt(val);
					datas.add(new IntegerData(i, itype));
				}
			} else if (type instanceof FloatType) {
				FloatType ftype = (FloatType) type;
				if (val.equals("null")) {
					datas.add(new FloatData((String)null, ftype));
				} else {
					float f = Float.parseFloat(val);
					datas.add(new FloatData(f, ftype));
				}
			} else if (type instanceof DecimalType) {
				DecimalType dtype = (DecimalType) type;
				if (val.equals("null")) {
					datas.add(new DecimalData(null, dtype));
				} else {
					datas.add(dtype.valueOf(val));
				}
			} else if (type instanceof BooleanType) {
				BooleanType btype = (BooleanType) type;
				if (val.equals("null")) {
					datas.add(new BooleanData((String)null, btype));
				} else {
					datas.add(btype.valueOf(val));
				}
			} else if (type instanceof DateTimeType) {
				DateTimeType dttype = (DateTimeType) type;
				if (val.equals("null")) {
					datas.add(new DateTimeData(null, dttype));
				} else {
					datas.add(dttype.valueOf(val));
				}
			} else if (type instanceof TimestampType) {
				TimestampType tstype = (TimestampType) type;
				if (val.equals("null")) {
					datas.add(new TimestampData(null, tstype));
				} else {
					datas.add(tstype.valueOf(val));
				}
			} else if (type instanceof CharType) {
				CharType ctype = (CharType) type;
				if (val.equals("null")) {
					datas.add(new CharData(null, ctype));
				} else {
					datas.add(ctype.valueOf(val));
				}
				
			} else if (type instanceof VarcharType) {
				VarcharType vctype = (VarcharType) type;
				if (val.equals("null")) {
					datas.add(new VarcharData(null, vctype));
				} else {
					datas.add(vctype.valueOf(val));		
				}
			}
		}
		return new Record(datas, s);
	}
	
	public void close() { 
		rf.close();
	}
	
	public void createIndex(String indexName, String fieldName) {
		
	}
	
	public void dropIndex(String indexName) {
		
	}
	
	/**
	 * Deletes all the content in the record file
	 */
	public void delete() {
		rf.clear();
	}
	
	public void deleteRecord(List<Record> records) {
		rf.beforeFirst();
		while (rf.hasNext()) {
			Record record = getRecordFromRid(rf.currentRid());
			int pos = records.indexOf(record);
			if (pos >= 0 && pos < records.size()) {
				rf.delete();
				records.remove(pos);
			}
			if (records.isEmpty()) {
				break;
			}
		}
	}
	
	public void updateRecord(Record record) {
		int pos = 0;
		for (Data data : record.getData()) {
			String fieldName = ti.schema().getFromColumn(pos).getColumnName();
			if (data == null) {
				rf.setString(fieldName, "null");
			} else {
				rf.setString(fieldName, data.storageValue());
			}
			pos++;
		}
	}
	
	public void drop() {

	}
	
	public Schema getSchema() {
		return ti.schema();
	}
	
	public void insertRecord(Record record) {
		rf.insert();
		int pos = 0;
		for (Data data : record.getData()) {
			String fieldName = ti.schema().getFromColumn(pos).getColumnName();
			if (data == null) {
				rf.setString(fieldName, "null");
			} else {
				rf.setString(fieldName, data.storageValue());
			}
			pos++;
		}
	}
	
	public List<Record> records() {
		List<Record> result = new ArrayList<Record>();
		rf.beforeFirst();
		while (rf.hasNext()) {
			result.add(getRecordFromRid(rf.currentRid()));
		}
		return result;
	}
	
	public void beforeFirst() {
		rf.beforeFirst();
	}
	
	public boolean hasNext() {
		return rf.hasNext();
	}
	
	public Record next() {
		return getRecordFromRid(rf.currentRid());
	}
	
}
