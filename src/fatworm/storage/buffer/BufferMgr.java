package fatworm.storage.buffer;

import fatworm.storage.file.*;

public class BufferMgr {

	private Buffer[] bufferPool;
	private int availableCount;
	
	public BufferMgr(int numbuffs) {
		bufferPool = new Buffer[numbuffs];
		availableCount = numbuffs;
		for (int i = 0; i < numbuffs; ++i) {
			bufferPool[i] = new Buffer();
		}
	}
	
	public void flushAll(int txnum) {
		for (Buffer buffer : bufferPool) {
			if (buffer.isModifiedBy(txnum)) {
				buffer.flush();
			}
		}
	}
	
	public Buffer pin(Block blk) {
		Buffer buffer = findExistingBuffer(blk);
		if (buffer == null) {
			buffer = chooseUnpinnedBuffer();
			if (buffer == null) {
				return null;
			}
			buffer.assignToBlock(blk);
		}
		if (!buffer.isPinned()) {
			availableCount--;
		}
		buffer.pin();
		return buffer;
	}
	
	public Buffer pinNew(String filename, PageFormatter fmtr) {
		Buffer buffer = chooseUnpinnedBuffer();
		if (buffer == null) {
			return null;
		}
		buffer.assignToNew(filename, fmtr);
		availableCount--;
		buffer.pin();
		return buffer;
	}
	
	public void unpin(Buffer buff) {
		buff.unpin();
		if (!buff.isPinned()) {
			availableCount++;
		}
	}
	
	public int available() {
		return availableCount;
	}
	
	private Buffer findExistingBuffer(Block blk) {
		for (Buffer buffer : bufferPool) {
			if (buffer.block() != null && buffer.block().equals(blk)) {
				return buffer;
			}
		}
		return null;
	}
	
	private Buffer chooseUnpinnedBuffer() {
		for (Buffer buffer : bufferPool) {
			if (!buffer.isPinned()) {
				return buffer;
			}
		}
		return null;
	}
}
