package fatworm.indexing.metadata;

import fatworm.indexing.index.Index;
import fatworm.indexing.schema.Schema;

public class IndexInfo {

    @SuppressWarnings("unused")
	private String indexName, fieldName;
    @SuppressWarnings("unused")
	private TableInfo ti;

    /**
     * Creates an IndexInfo object for the specified index.
     * @param idxname the name of the index
     * @param tblname the name of the table
     * @param fldname the name of the indexed field
     * @param tx the calling transaction
     */
    public IndexInfo(String idxname, String tblname, String fldname) {
    	this.indexName = tblname + "_" + idxname;
    	this.fieldName = fldname;
    }

    /**
     * Opens the index described by this object.
     * @return the Index object associated with this information
     */
    public Index open() {
    	return null;
    }

    /**
     * Returns the schema of the index records.
     * The schema consists of the dataRID (which is
     * represented as two integers, the block number and the
     * record ID) and the dataval (which is the indexed field).
     * Schema information about the indexed field is obtained
     * via the table's metadata.
     * @return the schema of the index records
     */
    @SuppressWarnings("unused")
	private Schema schema() {
    	return null;
    }
}
