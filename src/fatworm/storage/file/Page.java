package fatworm.storage.file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import fatworm.util.Fatworm;

public class Page {

    /**
     * The number of bytes in a block.
     * This value is set unreasonably low, so that it is easier
     * to create and test databases having a lot of blocks.
     * A more realistic value would be 4K.
     */
    public static final int BLOCK_SIZE = 4096;

    /**
     * The size of an integer in bytes.
     * This value is almost certainly 4, but it is
     * a good idea to encode this value as a constant. 
     */
    public static final int INT_SIZE = Integer.SIZE / Byte.SIZE;

    /**
     * The maximum size, in bytes, of a string of length n.
     * A string is represented as the encoding of its characters,
     * preceded by an integer denoting the number of bytes in this encoding.
     * If the JVM uses the US-ASCII encoding, then each char
     * is stored in one byte, so a string of n characters
     * has a size of 4+n bytes.
     * @param n the size of the string
     * @return the maximum number of bytes required to store a string of size n
     */
    public static final int STR_SIZE(int n) {
        float bytesPerChar = Charset.defaultCharset().newEncoder().maxBytesPerChar();
        return INT_SIZE + (n * (int)bytesPerChar);
    }

    private ByteBuffer contents = ByteBuffer.allocateDirect(BLOCK_SIZE);
    private FileMgr fileMgr = Fatworm.fileMgr();

    public Page() {}

    /**
     * Populates the page with the contents of the specified disk block. 
     * @param blk a reference to a disk block
     */
    public void read(Block blk) {
        fileMgr.read(blk, contents);
    }

    /**
     * Writes the contents of the page to the specified disk block.
     * @param blk a reference to a disk block
     */
    public void write(Block blk) {
        fileMgr.write(blk, contents);
    }

    /**
     * Appends the contents of the page to the specified file.
     * @param filename the name of the file
     * @return the reference to the newly-created disk block
     */
    public Block append(String filename) {
        return fileMgr.append(filename, contents);
    }

    /**
     * Returns the integer value at a specified offset of the page.
     * If an integer was not stored at that location, 
     * the behavior of the method is unpredictable.
     * @param offset the byte offset within the page
     * @return the integer value at that offset
     */
    public int getInt(int offset) {
        contents.position(offset);
        return contents.getInt();
    }

    /**
     * Writes an integer to the specified offset on the page.
     * @param offset the byte offset within the page
     * @param val the integer to be written to the page
     */
    public void setInt(int offset, int val) {
        contents.position(offset);
        contents.putInt(val);
    }

    /**
     * Returns the string value at the specified offset of the page.
     * If a string was not stored at that location,
     * the behavior of the method is unpredictable.
     * @param offset the byte offset within the page
     * @return the string value at that offset
     */
    public String getString(int offset) {
        contents.position(offset);
        int len = contents.getInt();
        byte[] buf = new byte[len];
        contents.get(buf);
        return new String(buf);
    }

    /**
     * Writes a string to the specified offset on the page.
     * @param offset the byte offset within the page
     * @param val the string to be written to the page
     */
    public void setString(int offset, String val) {
        contents.position(offset);
        contents.putInt(val.length());
        contents.put(val.getBytes());
    }
}
