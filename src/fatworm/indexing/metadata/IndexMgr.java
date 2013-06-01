package fatworm.indexing.metadata;

import java.util.*;

public class IndexMgr {

    @SuppressWarnings("unused")
	private TableInfo ti;

    /**
     * Creates the index manager.
     * This constructor is called during system startup.
     * If the database is new, then the <i>idxcat</i> table is created.
     * @param isnew indicates whether this is a new database
     * @param tx the system startup transaction
     */
    public IndexMgr(boolean isnew, TableMgr tblmgr) {
    	if (!isnew) {
    		
    	}
    }

    /**
     * Creates an index of the specified type for the specified field.
     * A unique ID is assigned to this index, and its information
     * is stored in the idxcat table.
     * @param idxname the name of the index
     * @param tblname the name of the indexed table
     * @param fldname the name of the indexed field
     * @param tx the calling transaction
     */
    public void createIndex(String idxname, String tblname, String fldname) {

    }

    /**
     * Returns a map containing the index info for all indexes
     * on the specified table.
     * @param tblname the name of the table
     * @param tx the calling transaction
     * @return a map of IndexInfo objects, keyed by their field names
     */
    public Map<String,IndexInfo> getIndexInfo(String tblname) {
    	return null;
    }
}
