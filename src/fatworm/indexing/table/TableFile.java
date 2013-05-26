package fatworm.indexing.table;

import java.math.BigInteger;
import java.util.*;

import fatworm.indexing.data.BooleanData;
import fatworm.indexing.data.BooleanType;
import fatworm.indexing.data.CharType;
import fatworm.indexing.data.Data;
import fatworm.indexing.data.DataType;
import fatworm.indexing.data.DateTimeType;
import fatworm.indexing.data.DecimalType;
import fatworm.indexing.data.FloatData;
import fatworm.indexing.data.FloatType;
import fatworm.indexing.data.IntegerData;
import fatworm.indexing.data.IntegerType;
import fatworm.indexing.data.TimestampType;
import fatworm.indexing.data.VarcharType;
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
			if (type instanceof IntegerType) {
				IntegerType itype = (IntegerType) type;
				datas.add(new IntegerData(rf.getInt(fieldName), itype));
			} else if (type instanceof FloatType) {
				byte[] buf = rf.getString(fieldName).getBytes();
				Float f = Float.intBitsToFloat(Lib.bytesToInt(buf, 0));
				FloatType ftype = (FloatType) type;
				datas.add(new FloatData(f, ftype));
			} else if (type instanceof DecimalType) {
				byte[] buf = rf.getString(fieldName).getBytes();
				String str = new BigInteger(buf).toString();
				DecimalType dtype = (DecimalType) type;
				datas.add(dtype.valueOf(str));
			} else if (type instanceof BooleanType) {
				byte[] buf = rf.getString(fieldName).getBytes();
				BooleanType btype = (BooleanType) type;
				datas.add(new BooleanData(buf[0] != 0, btype));
			} else if (type instanceof DateTimeType) {
				DateTimeType dttype = (DateTimeType) type;
				String str = rf.getString(fieldName);
				datas.add(dttype.valueOf(str));
			} else if (type instanceof TimestampType) {
				TimestampType dttype = (TimestampType) type;
				String str = rf.getString(fieldName);
				datas.add(dttype.valueOf(str));
			} else if (type instanceof CharType) {
				CharType ctype = (CharType) type;
				String str = rf.getString(fieldName);
				datas.add(ctype.valueOf(str));
			} else if (type instanceof VarcharType) {
				VarcharType vctype = (VarcharType) type;
				String str = rf.getString(fieldName);
				datas.add(vctype.valueOf(str));			
			}
		}
		return new Record(datas, s);
	}
	
	public void close() { }
	
	public void createIndex(String indexName, String fieldName) {
		
	}
	
	public void dropIndex(String indexName) {
		
	}
	
	public void delete() {
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
			DataType type = data.getType();
			String fieldName = ti.schema().getFromColumn(pos).getColumnName();
			if (type instanceof IntegerType) {
				rf.setInt(fieldName, (int)data.getValue());
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
			DataType type = data.getType();
			String fieldName = ti.schema().getFromColumn(pos).getColumnName();
			if (type instanceof IntegerType) {
				rf.setInt(fieldName, (int)data.getValue());
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
