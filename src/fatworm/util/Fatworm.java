package fatworm.util;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.antlr.runtime.ANTLRInputStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.tree.CommonTree;

import fatworm.engine.optimization.Optimizer;
import fatworm.engine.parser.FatwormLexer;
import fatworm.engine.parser.FatwormParser;
import fatworm.engine.plan.Plan;
import fatworm.engine.plan.Planner;
import fatworm.indexing.metadata.MetadataMgr;
import fatworm.indexing.scan.Scan;
import fatworm.indexing.table.Record;
import fatworm.storage.StorageMgr;
import fatworm.storage.buffer.BufferMgr;
import fatworm.storage.file.FileMgr;

public class Fatworm {
	public static int BUFFER_SIZE = 128;
	public static int txnum = 1;
	public static String homedir = null;
	public static String dbname = null;
	
	private static FileMgr fm;
	private static BufferMgr bm;
	private static MetadataMgr mdm;
	private static StorageMgr stm;
	
	public static List<Record> paths = new ArrayList<Record>();

	public static void main(String[] args) {
		// Do Nothing
	}
	
	public static void init(String dirname) {
		homedir = dirname;
		stm = new StorageMgr();
	}
	
	public static void openDataBase(String dbName, boolean create) {
		File dbDirectory = new File(homedir, dbName);
		if (!dbDirectory.exists() && !create) {
			throw new RuntimeException("no database " + dbName + " found");
		}
		if (bm != null) {
			if (!(create && fm.dbname().equals(dbName))) {
				bm.flushAll(txnum);
			}
		}
		fm = new FileMgr(dbName);
		bm = new BufferMgr(BUFFER_SIZE);
		mdm = new MetadataMgr(fm.isNew());
		dbname = dbName;
	}
	
	public static void dropAll() {
		mdm.dropAll();
		bm.flushAll(txnum);
		bm = null;
		mdm = null;
		fm = null;
		dbname = null;
	}
	
	public static CommonTree parseQuery(String query) throws Exception {
		java.io.InputStream inp = new java.io.ByteArrayInputStream(query.getBytes());
        ANTLRInputStream input = new ANTLRInputStream(inp);
        FatwormLexer lexer = new FatwormLexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        FatwormParser parser = new FatwormParser(tokens);
        CommonTree result = (CommonTree)parser.statement().getTree();   		
        return result;
	}
	
	public static Scan translateQuery(CommonTree tree) {
		Scan result = null;
		
		try {
			Planner planner = new Planner(tree);
			Plan plan = planner.generatePlan();
			Optimizer optimizer = new Optimizer(plan);
			plan = optimizer.optimize();
			result = plan.createScan();
		} catch (Exception ex) {
			throw new RuntimeException("cannot create scan");
		}
			
		return result;
	}
	
	public static FileMgr fileMgr() { return fm; }
	public static BufferMgr bufferMgr() { return bm; }
	public static MetadataMgr metadataMgr() { return mdm; }
	public static StorageMgr storageMgr() { return stm; }
	
}
