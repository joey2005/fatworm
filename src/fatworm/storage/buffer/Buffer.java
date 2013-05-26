package fatworm.storage.buffer;

import fatworm.storage.file.*;

public class Buffer {
	
	private Page contents = new Page();
	private Block block = null;
	private int pins = 0;
	private int modifiedBy = -1; // negative means not modified
	
	public Buffer() { }
	
	/**
    * Returns the integer value at the specified offset of the
    * buffer's page.
    * If an integer was not stored at that location,
    * the behavior of the method is unpredictable.
    * @param offset the byte offset of the page
    * @return the integer value at that offset
    */
	public int getInt(int offset) {
		return contents.getInt(offset);
	}
	
	/**
    * Returns the string value at the specified offset of the
    * buffer's page.
    * If a string was not stored at that location,
    * the behavior of the method is unpredictable.
    * @param offset the byte offset of the page
    * @return the string value at that offset
    */
	public String getString(int offset) {
		return contents.getString(offset);
	}
	
   /**
    * Writes an integer to the specified offset of the
    * buffer's page.

    * The buffer saves the id of the transaction

    * @param offset the byte offset within the page
    * @param val the new integer value to be written
    * @param txnum the id of the transaction performing the modification

    */
   public void setInt(int offset, int val, int txnum) {	
	   modifiedBy = txnum;
	   contents.setInt(offset, val);
   }
   
   /**
    * Writes a string to the specified offset of the
    * buffer's page.

    * The buffer saves the id of the transaction

    * @param offset the byte offset within the page
    * @param val the new string value to be written
    * @param txnum the id of the transaction performing the modification

    */
   public void setString(int offset, String val, int txnum) {
	   modifiedBy = txnum;
	   contents.setString(offset, val);
   }
   
   /**
    * Returns a reference to the disk block
    * that the buffer is pinned to.
    * @return a reference to a disk block
    */
   public Block block() {
	   return block;
   }
   
   /**
    * Writes the page to its disk block if the
    * page is dirty.

    */
   void flush() {
	   if (modifiedBy != -1) {
		   contents.write(block);
		   modifiedBy = -1;
	   }
   }
   
   /**
    * Increases the buffer's pin count.
    */
   void pin() {
	   ++pins;
   }
   
   /**
    * Decreases the buffer's pin count.
    */
   void unpin() {
	   --pins;
   }
   
   /**
    * Returns true if the buffer is currently pinned
    * (that is, if it has a nonzero pin count).
    * @return true if the buffer is pinned
    */
   boolean isPinned() {
	   return pins == 0;
   }
   
   /**
    * Returns true if the buffer is dirty
    * due to a modification by the specified transaction.
    * @param txnum the id of the transaction
    * @return true if the transaction modified the buffer
    */
   boolean isModifiedBy(int txnum) {
	   return modifiedBy == txnum;
   }
   
   /**
    * Reads the contents of the specified block into
    * the buffer's page.
    * If the buffer was dirty, then the contents
    * of the previous page are first written to disk.
    * @param b a reference to the data block
    */
   void assignToBlock(Block b) {
	   flush();
	   block = b;
	   contents.read(b);
	   pins = 0;
   }
   
   /**
    * Initializes the buffer's page according to the specified formatter,
    * and appends the page to the specified file.
    * If the buffer was dirty, then the contents
    * of the previous page are first written to disk.
    * @param filename the name of the file
    * @param fmtr a page formatter, used to initialize the page
    */
   void assignToNew(String filename, PageFormatter fmtr) {
	   flush();
	   fmtr.format(contents);
	   block = contents.append(filename);
	   pins = 0;
   }
}
