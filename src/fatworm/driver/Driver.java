package fatworm.driver;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class Driver implements java.sql.Driver {
	
	private static Driver driverInstance;
	
	static {
		try {
			driverInstance = new fatworm.driver.Driver();
			java.sql.DriverManager.registerDriver(driverInstance);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	} 

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		if (url.startsWith("jdbc:fatworm:")) {
			return true;
		}
		return false;
	}

	@Override
	public Connection connect(String url, Properties prop) throws SQLException {
		if (!acceptsURL(url)) {
			return null;
		}
		return new fatworm.driver.Connection();
	}

	@Override
	public int getMajorVersion() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMinorVersion() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String arg0, Properties arg1)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean jdbcCompliant() {
		// TODO Auto-generated method stub
		return false;
	}

}
