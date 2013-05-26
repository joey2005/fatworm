package fatworm.storage;

import java.util.*;

import fatworm.storage.buffer.Buffer;
import fatworm.storage.buffer.BufferMgr;
import fatworm.storage.buffer.PageFormatter;
import fatworm.storage.file.Block;
import fatworm.util.Fatworm;

class BufferList {
	private Map<Block, Buffer> buffers = new HashMap<Block, Buffer>();
	private List<Block> pins = new ArrayList<Block>();
	
	BufferList() { }
	
	BufferMgr bufferMgr() {
		return Fatworm.bufferMgr();
	}
	
	Buffer getBuffer(Block block) {
		return buffers.get(block);
	}
	
	void pin(Block block) {
		Buffer buffer = bufferMgr().pin(block);
		buffers.put(block, buffer);
		pins.add(block);
	}
	
	Block pinNew(String filename, PageFormatter fmtr) {
		Buffer buffer = bufferMgr().pinNew(filename, fmtr);
		Block block = buffer.block();
		buffers.put(block, buffer);
		pins.add(block);		
		return block;
	}
	
	void unpin(Block block) {
		Buffer buffer = buffers.get(block);
		bufferMgr().unpin(buffer);
		pins.remove(block);
		if (!pins.contains(block)) {
			buffers.remove(block);
		}
	}
	
	void unpinAll() {
		for (Block block : pins) {
			Buffer buffer = buffers.get(block);
			bufferMgr().unpin(buffer);
		}
		buffers.clear();
		pins.clear();
	}
}