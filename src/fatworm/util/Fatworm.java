package fatworm.util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;

import org.antlr.runtime.ANTLRInputStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.tree.CommonTree;

import fatworm.engine.parser.FatwormLexer;
import fatworm.engine.parser.FatwormParser;
import fatworm.engine.plan.Planner;
import fatworm.engine.plan.Plan;
import fatworm.indexing.metadata.MetadataMgr;
import fatworm.indexing.scan.Scan;
import fatworm.storage.StorageMgr;
import fatworm.storage.buffer.BufferMgr;
import fatworm.storage.file.FileMgr;
import fatworm.test.DriverTest;

public class Fatworm {
	public static int BUFFER_SIZE = 128;
	public static int txnum = 1;
	public static String homedir = null;
	
	private static FileMgr fm;
	private static BufferMgr bm;
	private static MetadataMgr mdm;
	private static StorageMgr stm;

	public static void main(String[] args) {
		Fatworm.init(args[0]);
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
		fm = new FileMgr(dbName);
		bm = new BufferMgr(BUFFER_SIZE);
		mdm = new MetadataMgr(fm.isNew());
	}
	
	public static CommonTree parseQuery(String query) throws Exception {
		//System.out.println(query);
		java.io.InputStream inp = new java.io.ByteArrayInputStream(query.getBytes());
        ANTLRInputStream input = new ANTLRInputStream(inp);
        FatwormLexer lexer = new FatwormLexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        FatwormParser parser = new FatwormParser(tokens);
        CommonTree result = (CommonTree)parser.statement().getTree();   		
        //System.out.println(result.toStringTree());
        return result;
	}
	
	public static Scan translateQuery(CommonTree tree) {
		Planner planner = new Planner(tree);
		Plan plan = null;
		Scan result = null;
		try {
			plan = planner.generatePlan();
			//System.out.println(plan.toString());
			result = plan.createScan();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return result;
	}
	
	public static FileMgr fileMgr() { return fm; }
	public static BufferMgr bufferMgr() { return bm; }
	public static MetadataMgr metadataMgr() { return mdm; }
	public static StorageMgr storageMgr() { return stm; }
}
