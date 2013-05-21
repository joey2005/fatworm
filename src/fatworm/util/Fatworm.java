package fatworm.util;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;

import org.antlr.runtime.ANTLRInputStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.tree.CommonTree;

import fatworm.engine.parser.FatwormLexer;
import fatworm.engine.parser.FatwormParser;
import fatworm.engine.plan.Planner;
import fatworm.engine.plan.Plan;
import fatworm.indexing.scan.Scan;
import fatworm.storage.transection.Transection;
import fatworm.test.DriverTest;

public class Fatworm {
	
	public static Transection tx = null;

	public static void main(String[] args) {
		new DriverTest().test();
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
}
