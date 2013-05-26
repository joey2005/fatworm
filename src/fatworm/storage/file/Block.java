package fatworm.storage.file;

public class Block {
    private String fileName;
    private int blockNum;

    /**
     * Constructs a block reference 
     * for the specified filename and block number.
     * @param filename the name of the file
     * @param blknum the block number
     */
    public Block(String filename, int blknum) {
        this.fileName = filename;
        this.blockNum = blknum;
    }

    /**
     * Returns the name of the file where the block lives.
     * @return the filename
     */
    public String fileName() {
        return fileName;
    }

    /**
     * Returns the location of the block within the file.
     * @return the block number
     */
    public int number() {
        return blockNum;
    }

    @Override
        public boolean equals(Object o) {
            Block block = (Block) o;
            return fileName.equals(block.fileName) && blockNum == block.blockNum;
        }

    @Override
        public String toString() {
            return "[file " + fileName + ", block " + blockNum + "]";
        }

    @Override
        public int hashCode() {
            return toString().hashCode();
        }
}
