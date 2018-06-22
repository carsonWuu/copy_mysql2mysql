package net.sp.init.truncate;

import java.sql.Connection;
import java.sql.SQLException;

import net.sp.init.mysql.mysqlInit;

import com.tonetime.commons.database.helper.DbHelper;
import com.tonetime.commons.database.helper.JdbcCallback;

//import net.sp.init.mysql.DBUtil;
//import com.tonetime.commons.database.helper.DbHelper;
//import com.tonetime.commons.database.helper.JdbcCallback;

public class Truncate {
	
	public static void main(String [] args){
		
		String[] str=new String[102];
		for(int i=0;i<100;i++){
			str[i]=String.valueOf("iov_track_"+i);
		}
		str[100]="wx_sub_dev";
		str[101]="wx_subscriber";
		for(int i=100;i<102;i++){
			final String sql_Delete = "truncate "+str[i];  
			
			try {
				DbHelper.execute(mysqlInit.getInstance().getMasterSource(), new JdbcCallback() {
					
					@Override
					public Object doInJdbc(Connection arg0) throws SQLException, Exception {
						// TODO Auto-generated method stub
						
						
						return DbHelper.executeUpdate(arg0,sql_Delete);
					}
				});
				System.out.println(str[i]+" has been deleted!");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		System.out.println("All are deleted!");
	}
}
