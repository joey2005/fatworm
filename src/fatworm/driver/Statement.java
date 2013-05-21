package fatworm.driver;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;

import org.antlr.runtime.tree.CommonTree;

import fatworm.indexing.scan.*;
import fatworm.util.Fatworm;

public class Statement implements java.sql.Statement {
	
	private Connection connection;
	private ResultSet lastResult;
	
	public Statement(Connection connection) {
		this.connection = connection;
		this.lastResult = null;
	}

	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cancel() throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void clearBatch() throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void clearWarnings() throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void closeOnCompletion() throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		if (lastResult != null) {
			lastResult.close();
		}
		
		CommonTree tree = null;
		try {
			tree = Fatworm.parseQuery(sql);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Scan scan = Fatworm.translateQuery(tree);
		
		if (scan instanceof Operation) {System.out.println("Is operation");
			Operation operation = (Operation) scan;
			operation.doit();
		}
		
		ResultSet result = new fatworm.driver.ResultSet(scan, connection);
		lastResult = result;
		
		return true;
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys)
			throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean execute(String sql, String[] columnNames)
			throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int[] executeBatch() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys)
			throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes)
			throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames)
			throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Connection getConnection() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getFetchDirection() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getFetchSize() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxRows() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getResultSetType() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getUpdateCount() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isClosed() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isPoolable() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setCursorName(String name) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setMaxFieldSize(int max) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setMaxRows(int max) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException {
		// TODO Auto-generated method stub
		
	}

}
