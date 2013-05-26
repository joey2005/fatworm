package fatworm.indexing.table;

import static fatworm.storage.file.Page.*;
import fatworm.indexing.metadata.TableInfo;
import fatworm.storage.buffer.Buffer;
import fatworm.storage.file.Block;
import fatworm.util.Fatworm;

public class RecordPage {
    public static final int EMPTY = 0, INUSE = 1;

    private Block block;
    private TableInfo ti;
    private int slotSize;
    private int currentSlot = -1;
    private int lastSlot = -1;

    /** Creates the record manager for the specified block.
     * The current record is set to be prior to the first one.
     * @param blk a reference to the disk block
     * @param ti the table's metadata
     */
    public RecordPage(Block blk, TableInfo ti) {
        this.block = blk;
        this.ti = ti;
        slotSize = ti.recordLength() + INT_SIZE;
        Fatworm.storageMgr().pin(block);
    }

    /**
     * Closes the manager, by unpinning the block.
     */
    public void close() {
        if (block != null) {
            Fatworm.storageMgr().unpin(block);
            block = null;
        }
    }

    /**
     * Moves to the next record in the block.
     * @return false if there is no next record.
     */
    public boolean next() {
    	return searchFor(INUSE);
    }

    /**
     * Returns the integer value stored for the
     * specified field of the current record.
     * @param fldname the name of the field.
     * @return the integer stored in that field
     */	
    public int getInt(String fieldName) {
    	int offset = fieldpos(fieldName);
    	return Fatworm.storageMgr().getInt(block, offset);
    }

    /**
     * Returns the string value stored for the
     * specified field of the current record.
     * @param fldname the name of the field.
     * @return the string stored in that field
     */
    public String getString(String fieldName) {
    	int offset = fieldpos(fieldName);
    	return Fatworm.storageMgr().getString(block, offset);
    }

    /**
     * Stores an integer at the specified field
     * of the current record.
     * @param fldname the name of the field
     * @param val the integer value stored in that field
     */	
    public void setInt(String fieldName, int val) {
    	int offset = fieldpos(fieldName);
    	Fatworm.storageMgr().setInt(block, offset, val);
    }

    /**
     * Stores a string at the specified field
     * of the current record.
     * @param fldname the name of the field
     * @param val the string value stored in that field
     */
    public void setString(String fldname, String val) {
    	int offset = fieldpos(fldname);
    	Fatworm.storageMgr().setString(block, offset, val);
    }

    /**
     * Deletes the current record.
     * Deletion is performed by just marking the record
     * as "deleted"; the current record does not change. 
     * To get to the next record, call next().
     */
    public void delete() {
    	int pos = currentpos();
    	if (pos >= 0 && pos + INT_SIZE <= BLOCK_SIZE) {
    		Fatworm.storageMgr().setInt(block, pos, EMPTY);
    	}
    }

    /**
     * Inserts a new, blank record somewhere in the page.
     * Return false if there were no available slots.
     * @return false if the insertion was not possible
     */
    public boolean insert() {
    	// insert to the bottom of the record page
    	boolean found = false;
    	lastSlot++;
    	while (lastSlot * slotSize + slotSize <= BLOCK_SIZE) {
    		int pos = lastSlot * slotSize;
    		if (Fatworm.storageMgr().getInt(block, pos) == EMPTY) {
    			found = true;
    			break;
    		}
    		lastSlot++;
    	}
    	if (found) {
    		moveToId(lastSlot);
    		int pos = currentpos();
    		Fatworm.storageMgr().setInt(block, pos, INUSE);
    	}
    	return found;
    }

    /**
     * Sets the current record to be the record having the
     * specified ID.
     * @param id the ID of the record within the page.
     */
    public void moveToId(int id) {
    	currentSlot = id;
    }

    /**
     * Returns the ID of the current record.
     * @return the ID of the current record
     */
    public int currentId() {
    	return currentSlot;
    }

    private int currentpos() {
    	return currentSlot * slotSize;
    }

    private int fieldpos(String fldname) {
    	int offset = INT_SIZE + ti.offset(fldname);
    	return currentpos() + offset;
    }

    private boolean isValidSlot() {
    	return currentpos() + slotSize <= BLOCK_SIZE;
    }

    private boolean searchFor(int flag) {
    	currentSlot++;
    	while (isValidSlot()) {
    		int pos = currentpos();
    		if (Fatworm.storageMgr().getInt(block, pos) == flag) {
    			return true;
    		}
    		currentSlot++;
    	}
    	return false;
    }
}
