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
		long start = System.currentTimeMillis();
		int max=1000000;
		
		int once=30000;
		for(int i=0;i<max;i+=once){
			try{
				
				
				
				final String sql="select * from iov_track_0 where n_id > 0 and n_id < "+once;
				//System.out.println(sql);
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
		}
		
		long end = System.currentTimeMillis();
		
		System.out.println((end-start)+"ms");
		
	}
}
