package net.sp.init.wx;



import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.swing.text.html.HTMLDocument.Iterator;

import net.sp.init.mysql.DBUtil;
import net.sp.init.mysql.mysqlInit;

import com.mysql.jdbc.PreparedStatement;
import com.tonetime.commons.database.DataSourceBuilder;
import com.tonetime.commons.database.helper.DbHelper;
import com.tonetime.commons.database.helper.JdbcCallback;

import net.sp.init.wx.*;


	public class initWx_sub_dev implements Runnable{
		
		
		private int Count;//一次分页的个数
		
		public initWx_sub_dev(int count) {
			
			this.Count=count;
		}
		
		@Override
		public void run(){
			int slaveMax=initWx_sub_dev.SlaveMax();
			int MasterMax=initWx_sub_dev.MasterMax();
			/*副表若为空则返回-1
			 * 当副表最大值小于主表最大值时，则进行拷贝，完全拷贝
			 */
			
			System.out.println(slaveMax+"==>"+MasterMax);
			if(slaveMax>MasterMax){
				//table has changed!
				// do delete then insert all element
				
				if(deleteMaster()){
					int count=0;
					
					while(count<slaveMax){
						
						try {
							InsertTable(count, count+Count);
							count+=Count;
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
			else{
				System.out.println("wx_sub_dev no change!");
			}
			
				
		}
		public static int SlaveMax(){
			//the biggest n_id of SlaveSource
			Map<String, Object> map=null;
			try {
				map=(Map<String, Object>) DbHelper.execute(mysqlInit.getInstance().getSlaveSource(), new JdbcCallback() {
					
					@Override
					public Object doInJdbc(Connection arg0) throws SQLException, Exception {
						// TODO Auto-generated method stub
						String sql="select n_id from wx_sub_dev order by n_id desc limit 1";
						
						return DbHelper.queryFor(arg0,sql);
					}
				});
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			 Number value =0;
			 if(map==null)return -1;
			 for (String key : map.keySet()) {
	             value =  (Number)map.get(key);
	             break;
			 }
			return (value.intValue());
			
		}
		
		public static int MasterMax(){
			//the biggest n_id of MasterSource
			
			Map<String, Object> map=null;
			try {
				map=(Map<String, Object>) DbHelper.execute(mysqlInit.getInstance().getMasterSource(), new JdbcCallback() {
					
					@Override
					public Object doInJdbc(Connection arg0) throws SQLException, Exception {
						// TODO Auto-generated method stub
						String sql="select n_id from wx_sub_dev order by n_id desc limit 1";
						return DbHelper.queryFor(arg0,sql);
					}
				});
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			 Number value =0;
			 if(map==null)return -1;
			 for (String key : map.keySet()) {
	             value =  (Number)map.get(key);
	             break;
			 }
			return (value.intValue());
			
		}
		public boolean deleteMaster(){
			final String sql_Delete = "truncate  wx_sub_dev";  
			
			try {
				DbHelper.execute(mysqlInit.getInstance().getMasterSource(), new JdbcCallback() {
					
					@Override
					public Object doInJdbc(Connection arg0) throws SQLException, Exception {
						// TODO Auto-generated method stub
						
						
						return DbHelper.executeUpdate(arg0,sql_Delete);
					}
				});
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        return true;
		}
		public void InsertTable(final int start,final int end) throws SQLException{//分页查出数据元
			
			
			
			/*copy iov_track_0 to iov_track_99
			 * 
			 */
			
			
			/*
			 * 1.select
			 */
			final String sql_Select="select * from wx_sub_dev  where n_id > "+ start+" and n_id <= "+end;
			
			
	        
	        List<Map<String,Object>> list=null;
	        try {
				list = (List<Map<String, Object>>) DbHelper.execute(mysqlInit.getInstance().getSlaveSource(),new JdbcCallback() {
					
					@Override
					public Object doInJdbc(Connection arg0) throws SQLException, Exception {
						// TODO Auto-generated method stub
						return DbHelper.queryForList(arg0, sql_Select);
					}
				});
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        
	        if(list==null||list.size()==0)return ;
	       
	        String sqlTemp="INSERT INTO wx_sub_dev(t_update_time,n_id,c_model,t_create_time,c_dev_name,c_open_id,c_photo,c_imei,c_illegal,c_sos,n_rate,c_video,c_app_code) VALUES ";
	        int i=0;
	        for(;i<list.size()-1;i++){
	        	//第n-1个
	        	Map<String,Object> map=list.get(i);
	        	java.util.Iterator<String> it = map.keySet().iterator();
	        	sqlTemp += "(";
	        	while(it.hasNext()){
	        		String key =it.next();
	        		
	        		String value;
	        		value = map.get(key)!=null ?map.get(key).toString():null;
	        		sqlTemp += "'" + value + "'";
	        		if(it.hasNext()){
	        			sqlTemp += ",";
	        		}
	        	}
	        	sqlTemp += "),";
	        }
	        if(i != 0 ||list.size()==1){
	        	//第n个
	        	
	        	Map<String,Object> map=list.get(i);
	        	java.util.Iterator<String> it = map.keySet().iterator();
	        	
	        	sqlTemp += "(";
	        	while(it.hasNext()){
	        		String key =it.next();
	        		
	        		String value;
	        		value = map.get(key)!=null ?map.get(key).toString():null;
	        		sqlTemp  += "'" + value + "'";
	        		if(it.hasNext()){
	        			sqlTemp += ",";
	        		}
	        	}
	        	sqlTemp += ")";
	        }
	       
	        /*
	         * 2.Insert
	         */
	        
	        final String sql_Insert =sqlTemp; 
	        
	        System.out.println(sqlTemp);
	        insert(sql_Insert);
	        
	        
	    }
			
		public void insert(final String sql){
			try {
				DbHelper.execute(mysqlInit.getInstance().getMasterSource(),new JdbcCallback() {
					
					@Override
					public Object doInJdbc(Connection arg0) throws SQLException, Exception {
						// TODO Auto-generated method stub
						return DbHelper.executeUpdate(arg0,sql);
					}
				});
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		public static void main(String []args) throws Exception{
			//initCopy.number(0);
			System.out.println("start!");
			long start = System.currentTimeMillis(); //程序开始记录时间
			
			ExecutorService exec=Executors.newFixedThreadPool(102);
			//while(true){
			
			exec.execute(new initWx_sub_dev(1000));
			//exec.execute(new initWx_subscriber(1000));
			//System.out.println(start1-start);
			 
	        exec.shutdown();
			
	         
	        while(true){
	             if(exec.isTerminated()){
	                 //System.out.println("Finally do something ");
	                 long end = System.currentTimeMillis();
	                 System.out.println("用时: " + (end - start) + "ms");
	                 
	                 
	                 break;
	             }

	         }
		
			
		}
			
		
	}

