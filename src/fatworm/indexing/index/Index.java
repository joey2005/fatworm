package fatworm.indexing.index;

import fatworm.engine.predicate.ConstantPredicate;
import fatworm.indexing.table.RID;

public interface Index {
    /**
     * Positions the index before the first record
     * having the specified search key.
     * @param searchkey the search key value.
     */
    public void beforeFirst(ConstantPredicate searchkey);

    /**
     * Moves the index to the next record having the
     * search key specified in the beforeFirst method. 
     * Returns false if there are no more such index records.
     * @return false if no other index records have the search key.
     */
    public boolean next();

    /**
     * Returns the dataRID value stored in the current index record. 
     * @return the dataRID stored in the current index record.
     */
    public RID getDataRid();

    /**
     * Inserts an index record having the specified
     * dataval and dataRID values.
     * @param dataval the dataval in the new index record.
     * @param datarid the dataRID in the new index record.
     */
    public void insert(ConstantPredicate dataval, RID datarid);

    /**
     * Deletes the index record having the specified
     * dataval and dataRID values.
     * @param dataval the dataval of the deleted index record
     * @param datarid the dataRID of the deleted index record
     */
    public void delete(ConstantPredicate dataval, RID datarid);

    /**
     * Closes the index.
     */
    public void close();
}
