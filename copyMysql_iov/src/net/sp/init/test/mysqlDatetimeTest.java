package net.sp.init.test;

import java.sql.Connection;
import java.sql.SQLException;

import net.sp.init.mysql.mysqlInit;

import com.tonetime.commons.database.helper.DbHelper;
import com.tonetime.commons.database.helper.JdbcCallback;

//import net.sp.init.mysql.DBUtil;
//import com.tonetime.commons.database.helper.DbHelper;
//import com.tonetime.commons.database.helper.JdbcCallback;

public class mysqlDatetimeTest {
	public static void main(String [] args){
		try{
			String s="insert into test values(4,";
			String q="null";
			final String sql=s+q+")";
			DbHelper.execute(mysqlInit.getInstance().getMasterSource(),new JdbcCallback() {
				
				@Override
				public Object doInJdbc(Connection arg0) throws SQLException, Exception {
					// TODO Auto-generated method stub
					return DbHelper.executeUpdate(arg0,sql);
				}
			});
		}
		catch(Exception e){
			
		}
		finally{
			final String sql="select *from test";
			Object obj=null;
			try {
				obj = DbHelper.execute(mysqlInit.getInstance().getMasterSource(),new JdbcCallback() {
					
					@Override
					public Object doInJdbc(Connection arg0) throws SQLException, Exception {
						// TODO Auto-generated method stub
						return DbHelper.queryForValue(arg0,sql);
					}
				});
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println(obj);
		}
	}
}
