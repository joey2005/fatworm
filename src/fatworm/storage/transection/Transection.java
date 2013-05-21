package fatworm.storage.transection;

import fatworm.indexing.metadata.IndexMgr;
import fatworm.indexing.metadata.InfoMgr;
import fatworm.indexing.metadata.TableMgr;

public class Transection {
	
	public String dbName;
	public IndexMgr indexMgr;
	public InfoMgr infoMgr;
	public TableMgr tableMgr;

	public Transection(String dbName) {
		this.dbName = dbName;
		this.indexMgr = new IndexMgr();
		this.infoMgr = new InfoMgr();
		this.tableMgr = new TableMgr();
	}
}
