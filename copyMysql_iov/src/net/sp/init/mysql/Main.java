package net.sp.init.mysql;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import com.tonetime.commons.database.helper.*;
public class Main {
	public static void main(String[] args){
		List <Map<String,Object>> list = null ;
		try {
			list = (List<Map<String,Object>>) DbHelper.execute(mysqlInit.getInstance().getSlaveSource(),new JdbcCallback() {
				
				@Override
				public Object doInJdbc(Connection arg0) throws SQLException, Exception {
					// TODO Auto-generated method stub
					final String sql="select *  from iov_track_"+0;
					return DbHelper.queryForList(arg0, sql);
				}
			});
		} 
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(list.size());
	}
}
