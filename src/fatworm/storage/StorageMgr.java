package fatworm.storage;

import java.io.File;
import java.util.*;

import fatworm.indexing.metadata.MetadataMgr;
import fatworm.storage.buffer.Buffer;
import fatworm.storage.buffer.BufferMgr;
import fatworm.storage.buffer.PageFormatter;
import fatworm.storage.file.Block;
import fatworm.storage.file.FileMgr;
import fatworm.util.Fatworm;

public class StorageMgr {

    private static final int END_OF_FILE = -1;

    private int txnum;
    private BufferList myBuffers = new BufferList();

    public StorageMgr() {
        txnum = Fatworm.txnum;
    }
    
    public void openDataBase(String dbName, boolean create) {
    	Fatworm.openDataBase(dbName, create);
    }
    
    public void dropDataBase(String dbName) {
    	File dbDirectory = new File(Fatworm.homedir, dbName);
    	if (dbDirectory.exists()) {
    		for (String fileName : dbDirectory.list()) {
    			new File(dbDirectory, fileName).delete();
    		}
    		dbDirectory.delete();
    	}
    }

    /**
     * Pins the specified block.
     * The StorageMgr manages the buffer for the client.
     * @param blk a reference to the disk block
     */
    public void pin(Block blk) {
        myBuffers.pin(blk);
    }

    /**
     * Unpins the specified block.
     * The StorageMgr looks up the buffer pinned to this block,
     * and unpins it.
     * @param blk a reference to the disk block
     */
    public void unpin(Block blk) {
        myBuffers.unpin(blk);
    }

    /**
     * Returns the integer value stored at the
     * specified offset of the specified block.
     * The method calls the buffer to retrieve the value.
     * @param blk a reference to a disk block
     * @param offset the byte offset within the block
     * @return the integer stored at that offset
     */
    public int getInt(Block blk, int offset) {
        Buffer buff = myBuffers.getBuffer(blk);
        return buff.getInt(offset);
    }

    /**
     * Returns the string value stored at the
     * specified offset of the specified block.
     * The method calls the buffer to retrieve the value.
     * @param blk a reference to a disk block
     * @param offset the byte offset within the block
     * @return the string stored at that offset
     */
    public String getString(Block blk, int offset) {
        Buffer buff = myBuffers.getBuffer(blk);
        return buff.getString(offset);
    }

    /**
     * Stores an integer at the specified offset 
     * of the specified block.
     * The method reads the current value at that offset,
     * Finally, it calls the buffer to store the value,
     * @param blk a reference to the disk block
     * @param offset a byte offset within that block
     * @param val the value to be stored
     */
    public void setInt(Block blk, int offset, int val) {
        Buffer buff = myBuffers.getBuffer(blk);
        buff.setInt(offset, val, txnum);
    }

    /**
     * Stores a string at the specified offset 
     * of the specified block.
     * The method reads the current value at that offset, 
     * Finally, it calls the buffer to store the value,
     * @param blk a reference to the disk block
     * @param offset a byte offset within that block
     * @param val the value to be stored
     */
    public void setString(Block blk, int offset, String val) {
        Buffer buff = myBuffers.getBuffer(blk);
        buff.setString(offset, val, txnum);
    }

    /**
     * Returns the number of blocks in the specified file.
     * @param filename the name of the file
     * @return the number of blocks in the file
     */
    public int size(String filename) {
        return Fatworm.fileMgr().size(filename);
    }

    /**
     * Appends a new block to the end of the specified file
     * and returns a reference to it.
     * @param filename the name of the file
     * @param fmtr the formatter used to initialize the new page
     * @return a reference to the newly-created disk block
     */
    public Block append(String filename, PageFormatter fmtr) {
        Block blk = myBuffers.pinNew(filename, fmtr);
        unpin(blk);
        return blk;
    }
}
