package fatworm.indexing.table;

import fatworm.indexing.metadata.TableInfo;
import fatworm.storage.file.Block;
import fatworm.util.Fatworm;

public class RecordFile {

	private TableInfo ti;
	private String filename;
	private RecordPage rp;
	private int currentblocknum;
	
	public RecordFile(TableInfo ti) {
		this.ti = ti;
		filename = ti.fileName();
		if (Fatworm.storageMgr().size(filename) == 0) {
			appendBlock();
		}
		moveTo(0);
	}
	
	public void close() {
		rp.close();
	}
	
	public void beforeFirst() {
		moveTo(0);
	}
	
	public boolean hasNext() {
		while (true) {
			if (rp.next()) {
				return true;
			}
			if (atLastBlock()) {
				return false;
			}
			moveTo(currentblocknum + 1);
		}
	}
	
	public int getInt(String fieldName) {
		return rp.getInt(fieldName);
	}
	
	public void setInt(String fieldName, int val) {
		rp.setInt(fieldName, val);
	}
	
	public String getString(String fieldName) {
		return rp.getString(fieldName);
	}
	
	public void setString(String fieldName, String val) {
		rp.setString(fieldName, val);
	}
	
	public void delete() {
		rp.delete();
	}
	
	public void clear() {
		moveTo(0);
		while (true) {
			rp.clear();
			if (atLastBlock()) {
				break;
			}
			moveTo(currentblocknum + 1);
		}
	}
	
	public void insert() {
		while (!rp.insert()) {
			if (atLastBlock()) {
				appendBlock();
			}
			moveTo(currentblocknum + 1);
		}
	}
	
	public void moveToRid(RID rid) {
		moveTo(rid.blocknum());
		rp.moveToId(rid.id());
	}
	
	public RID currentRid() {
		return new RID(currentblocknum, rp.currentId());
	}
	
	private void moveTo(int blocknum) {
		if (rp != null) {
			rp.close();
		}
		currentblocknum = blocknum;
		Block block = new Block(filename, currentblocknum);
		rp = new RecordPage(block, ti);
	}
	
	private boolean atLastBlock() {
		return currentblocknum == Fatworm.storageMgr().size(filename) - 1;
	}
	
	private void appendBlock() {
		RecordFormatter fmtr = new RecordFormatter(ti);
		Fatworm.storageMgr().append(filename, fmtr);
	}
}
