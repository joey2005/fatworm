package fatworm.storage.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import fatworm.util.Fatworm;

public class FileMgr {

    private String alias;
    private File dbDirectory;
    private boolean isNew;
    private Map<String, FileChannel> openFiles = new HashMap<String, FileChannel>();

    /**
     * Creates a file manager for the specified database.
     * The database will be stored in a folder of that name
     * in the user's home directory.
     * If the folder does not exist, then a folder containing
     * an empty database is created automatically.
     * Files for all temporary tables (i.e. tables beginning with "temp") are deleted.
     * @param dbname the name of the directory that holds the database
     */
    public FileMgr(String dbName) {
        alias = dbName;
        dbDirectory = new File(Fatworm.homedir, dbName);
        isNew = !dbDirectory.exists();

        if (isNew && !dbDirectory.mkdir()) {
            throw new RuntimeException("cannot create " + dbName);
        }

        for (String fileName : dbDirectory.list()) {
            if (fileName.startsWith("temp")) {
                new File(dbDirectory, fileName).delete();
            }
        }
    } 

    public String dbname() {
        return alias;
    }

    /**
     * Reads the contents of a disk block into a bytebuffer.
     * @param blk a reference to a disk block
     * @param bb  the bytebuffer
     */
    void read(Block blk, ByteBuffer bb) {
        try {
            bb.clear();
            FileChannel fc = getFile(blk.fileName());
            fc.read(bb, blk.number() * Page.BLOCK_SIZE);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            throw new RuntimeException("cannot read block " + blk);
        }
    }

    /**
     * Writes the contents of a bytebuffer into a disk block.
     * @param blk a reference to a disk block
     * @param bb  the bytebuffer
     */
    void write(Block blk, ByteBuffer bb) {
        try {
            bb.clear();
            FileChannel fc = getFile(blk.fileName());
            fc.write(bb, blk.number() * Page.BLOCK_SIZE);
        } catch (IOException ex) {
            throw new RuntimeException("cannot write block " + blk);
        }
    }

    /**
     * Appends the contents of a bytebuffer to the end
     * of the specified file.
     * @param filename the name of the file
     * @param bb  the bytebuffer
     * @return a reference to the newly-created block.
     */
    Block append(String filename, ByteBuffer bb) {
        int pos = size(filename);
        Block block = new Block(filename, pos);
        write(block, bb);
        return block;
    }

    /**
     * Returns the number of blocks in the specified file.
     * @param filename the name of the file
     * @return the number of blocks in the file
     */
    public int size(String filename) {
        try {
            FileChannel fc = getFile(filename);
            return (int)(fc.size() / Page.BLOCK_SIZE);
        } catch (IOException ex) {
            throw new RuntimeException("cannot access file " + filename);
        }
    }

    /**
     * Returns a boolean indicating whether the file manager
     * had to create a new database directory.
     * @return true if the database is new
     */
    public boolean isNew() {
        return isNew;
    }

    /**
     * Returns the file channel for the specified filename.
     * The file channel is stored in a map keyed on the filename.
     * If the file is not open, then it is opened and the file channel
     * is added to the map.
     * @param filename the specified filename
     * @return the file channel associated with the open file.
     * @throws IOException
     */
    private FileChannel getFile(String filename) throws IOException {
        if (!openFiles.containsKey(filename)) {
            File dbTable = new File(dbDirectory, filename);
            RandomAccessFile f = new RandomAccessFile(dbTable, "rws");
            FileChannel fc = f.getChannel();
            openFiles.put(filename, fc);
            return fc;
        } else {
            return openFiles.get(filename);
        }
    }
}
