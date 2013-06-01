package fatworm.indexing.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import fatworm.indexing.data.CharType;
import fatworm.indexing.data.Data;
import fatworm.indexing.data.DataType;
import fatworm.indexing.data.IntegerType;
import fatworm.indexing.schema.AttributeField;
import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.RecordFile;
import fatworm.indexing.table.TableFile;

public class TableMgr {

    private Map<String, TableInfo> tableInfos;

    private static final int MAXLEN = 200;

    private TableInfo fcatInfo, fakeInfo;

    public TableMgr(boolean isnew) {		
        tableInfos = new HashMap<String, TableInfo>();

        List<AttributeField> fields = new ArrayList<AttributeField>();
        fields.add(new AttributeField("tblname", new CharType(MAXLEN)));
        fields.add(new AttributeField("fldname", new CharType(MAXLEN)));
        fields.add(new AttributeField("type", new IntegerType()));
        fields.add(new AttributeField("null", new IntegerType()));
        fields.add(new AttributeField("default", new CharType(MAXLEN)));
        fields.add(new AttributeField("autoinc", new IntegerType()));
        Schema fcatSchema = new Schema("fldcat", fields);
        fcatInfo = new TableInfo("fldcat", fcatSchema);
        
        List<AttributeField> fields2 = new ArrayList<AttributeField>();
        fields2.add(new AttributeField("fake", new CharType(MAXLEN)));
        Schema fakeSchema = new Schema("fakeTable", fields2);
        fakeInfo = new TableInfo("fakeTable", fakeSchema);
        
        
        if (!isnew) {
            RecordFile fcatfile = new RecordFile(fcatInfo);
            Map<String, List<AttributeField>> mapping = new HashMap<String, List<AttributeField>>();
            while (fcatfile.hasNext()) {
                String tableName = fcatfile.getString("tblname");
                String fieldname = fcatfile.getString("fldname");
                DataType type = DataType.decode(fcatfile.getInt("type"));
                int isnull = fcatfile.getInt("null");
                String str = fcatfile.getString("default");
                Data defaultvalue = null;
                if (!str.equals("") && str.length() > 0) {
                	defaultvalue = type.valueOf(str);
                }
                boolean autoinc = fcatfile.getInt("autoinc") == 1;
                AttributeField af = new AttributeField(fieldname, type, isnull, defaultvalue, autoinc);
                
                if (mapping.containsKey(tableName)) {
                	List<AttributeField> attributes = mapping.get(tableName);
                	attributes.add(af);
                } else {
                	List<AttributeField> attributes = new ArrayList<AttributeField>();
                	attributes.add(af);
                	mapping.put(tableName, attributes);
                }
            }
            fcatfile.close();
            
            for (String tblname : mapping.keySet()) {
            	Schema schema = new Schema(tblname, mapping.get(tblname));
            	TableInfo ti = new TableInfo(tblname, schema);
                tableInfos.put(tblname, ti);
            }
        }
        
        if (!tableInfos.containsKey("fldcat")) {
        	createTable("fldcat", fcatSchema);
        }
        
        if (!tableInfos.containsKey("fakeTable")) {
        	createTable("fakeTable", fakeSchema);
        	
		    RecordFile fakefile = new RecordFile(fakeInfo);
		    fakefile.insert();
		    fakefile.setString("fake", "0");
		    fakefile.close();
        }
    }

    public TableInfo createTable(String tableName, Schema schema) {
        TableInfo ti = new TableInfo(tableName, schema);
        tableInfos.put(tableName, ti);

        // insert a record into fldcat for each field
        RecordFile fcatfile = new RecordFile(fcatInfo);
        for (AttributeField af : schema.getAllFields()) {
            String fldname = af.getColumnName();
            fcatfile.insert();
            fcatfile.setString("tblname", tableName);
            fcatfile.setString("fldname", fldname);
            DataType type = schema.getFromVariableName(fldname).getType();
            fcatfile.setInt("type", type.encode());
            fcatfile.setInt("null", af.isNull);
            if (af.defaultValue == null) {
            	fcatfile.setString("default", "");
            } else {
            	fcatfile.setString("default", af.defaultValue.toString());
            }
            fcatfile.setInt("autoinc", af.autoIncrement ? 1 : 0);
        }
        fcatfile.close();

        return ti;
    }

    public TableInfo getTableInfo(String tableName) {
        return tableInfos.get(tableName);
    }

    public TableFile getTableFileAccess(String tableName) {
        return new TableFile(getTableInfo(tableName));
    }

    public void dropTable(List<String> tableNameList) {
        RecordFile fcatfile = new RecordFile(fcatInfo);
        fcatfile.beforeFirst();
        while (fcatfile.hasNext()) {
        	String tableName = fcatfile.getString("tblname");
        	int pos = tableNameList.indexOf(tableName);
        	if (pos >= 0 && pos < tableNameList.size()) {
        		fcatfile.delete();
        	}
        }
        fcatfile.close();
        
        for (String tableName : tableNameList) {
        	TableInfo ti = tableInfos.get(tableName);
        	if (ti != null) {
        		TableFile tf = new TableFile(ti);
        		tf.delete();
            	tf.close();
        	}
            tableInfos.remove(tableName);
        }
    }
    
    public void dropAll() {
        RecordFile fcatfile = new RecordFile(fcatInfo);
        fcatfile.clear();
        fcatfile.close();
        
        for (String tableName : tableInfos.keySet()) {
        	TableInfo ti = tableInfos.get(tableName);
        	if (ti != null) {
        		TableFile tf = new TableFile(ti);
        		tf.delete();
            	tf.close();
        	}
        }	
        tableInfos.clear();
    }
}
