package fatworm.indexing.table;

public class RID {
	private int blocknum;
	private int id;
	
	public RID(int blocknum, int id) {
		this.blocknum = blocknum;
		this.id = id;
	}
	
	public int blocknum() {
		return blocknum;
	}
	
	public int id() {
		return id;
	}
	
	@Override
	public boolean equals(Object o) {
		RID rid = (RID) o;
		return blocknum == rid.blocknum && id == rid.id;
		
	}
	
	@Override
	public String toString() {
		return "RID[" + blocknum + ", " + id + "]";
	}
}
