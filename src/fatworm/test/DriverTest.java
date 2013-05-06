package fatworm.test;

import java.sql.DriverManager;
import java.sql.SQLException;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Enumeration;


public class DriverTest {

	public void test() {
		
		try {
			Class.forName("fatworm.driver.Driver");
			Connection connection = DriverManager.getConnection("jdbc:fatworm://localhost");
			Statement statement = connection.createStatement();
			
			statement.execute("insert into test1 values(1,111111111111111)");
//			statement.execute("SELECT MODEL306.IS_MUTAGEN, COUNT( MODEL306.MODEL_ID ) FROM MODEL AS MODEL306, BOND AS T1008290441960  WHERE MODEL306.MODEL_ID=T1008290441960.MODEL_ID AND MODEL306.LOGP='0' GROUP BY MODEL306.IS_MUTAGEN ORDER BY MODEL306.IS_MUTAGEN ASC");
			//statement.execute("delete from test1 where (a=1 and b=9) or (a=2 or b=8)");
			//statement.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
}
