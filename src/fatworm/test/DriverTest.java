package fatworm.test;

import java.sql.DriverManager;
import java.sql.SQLException;

import java.sql.Connection;
import java.sql.Statement;

import java.sql.ResultSet;

public class DriverTest {

	public void test() {
		
		try {
			Class.forName("fatworm.driver.Driver");
			Connection connection = DriverManager.getConnection("jdbc:fatworm:/D:\\workspace\\fatworm-sister\\db");
			Statement stmt = connection.createStatement();
			
			stmt.execute("drop database tmp");
			stmt.execute("create database tmp");
			stmt.execute("use tmp");
			stmt.execute("create table test1 ( a int )");
			stmt.execute("insert into test1 values(1)");
			stmt.execute("drop database tmp");
			stmt.execute("create database tmp");
			stmt.execute("use tmp");
			stmt.execute("create table test1 ( a int )");
			stmt.execute("insert into test1 values(1)");			
			stmt.execute("select * from test1");
			ResultSet rs = stmt.getResultSet();
			rs.beforeFirst();
			while (rs.next()) {
				System.out.println(rs.getObject(1));
			}
			
			stmt.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
}
