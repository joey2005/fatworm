package fatworm.storage.buffer;

import fatworm.storage.file.Block;

public class BufferMgr {

	private Buffer[] bufferPool;
	private int availableCount;
	
	//private Map<Block, Buffer> managers;
	//private Set<Buffer> emptyBuffers;
	
	public BufferMgr(int numbuffs) {
		bufferPool = new Buffer[numbuffs];
		availableCount = numbuffs;
		for (int i = 0; i < numbuffs; ++i) {
			bufferPool[i] = new Buffer();
		}
		/*
		managers = new HashMap<Block, Buffer>();
		emptyBuffers = new HashSet<Buffer>();
		for (int i = 0; i < numbuffs; ++i) {
			emptyBuffers.add(bufferPool[i]);
		}*/
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
			//managers.remove(buffer.block());
			buffer.assignToBlock(blk);
			//managers.put(blk, buffer);
		}
		if (!buffer.isPinned()) {
			//emptyBuffers.remove(buffer);
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
		//emptyBuffers.remove(buffer);
		availableCount--;
		buffer.pin();
		return buffer;
	}
	
	public void unpin(Buffer buff) {
		buff.unpin();
		if (!buff.isPinned()) {
			//emptyBuffers.add(buff);
			availableCount++;
		}
	}
	
	public int available() {
		return availableCount;
	}
	
	private Buffer findExistingBuffer(Block blk) {
		//Buffer buffer = managers.get(blk);
		//return buffer;
		for (Buffer buffer : bufferPool) {
			if (buffer.block() != null && buffer.block().equals(blk)) {
				return buffer;
			}
		}
		return null;
	}
	
	private Buffer chooseUnpinnedBuffer() {
		/*
		Buffer buffer = null;
		if (emptyBuffers.size() > 0) {
			Iterator<Buffer> it = emptyBuffers.iterator();
			buffer = it.next();
		}
		return buffer;*/
		for (Buffer buffer : bufferPool) {
			if (!buffer.isPinned()) {
				return buffer;
			}
		}
		return null;
	}
}
