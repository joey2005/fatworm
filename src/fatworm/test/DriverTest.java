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
			
			statement.execute("SELECT QUUX.SHIP FROM " +
							"(SELECT SHIP, BATTLE2, DATE AS DATE1 FROM " +
							"(SELECT FOO.SHIP AS SHIP, FOO.BATTLE AS BATTLE1, BAR.BATTLE AS BATTLE2 FROM " +
							"(SELECT * FROM Outcomes AS O1 WHERE O1.RESULT = 'damaged') AS FOO, " +
							"(SELECT * FROM Outcomes AS O2 ) AS BAR " +
							"WHERE FOO.BATTLE <> BAR.BATTLE AND FOO.SHIP = BAR.SHIP ) AS BAT, " +
							"(SELECT * FROM Battles ) AS QUX " +
							"WHERE QUX.NAME = BAT.BATTLE1) AS QUUX, " +
							"(SELECT * FROM Battles ) AS QUUUX " +
							"WHERE QUUUX.NAME = QUUX.BATTLE2 AND QUUUX.DATE > QUUX.DATE1");
			//statement.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
}
