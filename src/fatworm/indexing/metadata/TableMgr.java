package fatworm.indexing.metadata;

import java.util.*;

import fatworm.indexing.LogicalFileMgr;
import fatworm.indexing.data.CharType;
import fatworm.indexing.data.Data;
import fatworm.indexing.data.DataType;
import fatworm.indexing.data.IntegerType;
import fatworm.indexing.schema.AttributeField;
import fatworm.indexing.schema.Schema;
import fatworm.indexing.table.Record;
import fatworm.indexing.table.RecordFile;
import fatworm.indexing.table.TableFile;
import fatworm.util.Fatworm;

public class TableMgr {

    private Map<String, TableInfo> tableInfos;

    private static final int MAXLEN = 20;

    private TableInfo fcatInfo;

    public TableMgr(boolean isnew) {		
        tableInfos = new HashMap<String, TableInfo>();

        List<AttributeField> fields = new ArrayList<AttributeField>();
        fields.add(new AttributeField("tblname", new CharType(MAXLEN)));
        fields.add(new AttributeField("fldname", new CharType(MAXLEN)));
        fields.add(new AttributeField("type", new IntegerType()));
        fields.add(new AttributeField("null", new IntegerType()));
        fields.add(new AttributeField("default", new CharType(MAXLEN * 4)));
        fields.add(new AttributeField("autoinc", new IntegerType()));
        Schema fcatSchema = new Schema("fldcat", fields);
        fcatInfo = new TableInfo("fldcat", fcatSchema);

        if (isnew) {
            createTable("fldcat", fcatSchema);
        } else {
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
            tableInfos.remove(tableName);
        }
    }
}
