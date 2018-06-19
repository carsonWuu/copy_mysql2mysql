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

import com.tonetime.commons.database.DataSourceBuilder;
import com.tonetime.commons.database.helper.DbHelper;
import com.tonetime.commons.database.helper.JdbcCallback;

import net.sp.init.wx.*;


	public class initWx_subscriber implements Runnable{
		
		
		private int Count;//һ�η�ҳ�ĸ���
		
		public initWx_subscriber(int count) {
			
			this.Count=count;
		}
		
		@Override
		public void run(){
			int slaveMax=SlaveMax();
			int MasterMax=MasterMax();
			/*������Ϊ���򷵻�-1
			 * ���������ֵС���������ֵʱ������п�������ȫ����
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
				System.out.println("wx_subscriber no change!");
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
						String sql="select n_id from wx_subscriber order by n_id desc limit 1";
						
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
						String sql="select n_id from wx_subscriber order by n_id desc limit 1";
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
			final String sql_Delete = "truncate  wx_subscriber";  
			
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
		public void InsertTable(final int start,final int end) throws SQLException{//��ҳ�������Ԫ
			
			
			
			/*copy iov_track_0 to iov_track_99
			 * 
			 */
			
			
			/*
			 * 1.select
			 */
			final String sql_Select="select * from wx_subscriber  where n_id > "+ start+" and n_id <= "+end;
			
			
	        
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
	       
	        String sqlTemp="INSERT INTO wx_subscriber("
	        		+ "t_update_time,n_id,n_is_sub,t_create_time,n_precision,n_sex,n_lat,n_lng,c_country,c_open_id,c_city,c_province,c_union_id,t_unsub_time,"
	        		+ "c_lac_label,c_app_code,c_avatar,c_wx_nickname,c_mobile,t_sub_time,c_wx_num) "
	        		+ "VALUES ";
	        int i=0;
	        for(;i<list.size()-1;i++){
	        	//��n-1��
	        	Map<String,Object> map=list.get(i);
	        	java.util.Iterator<String> it = map.keySet().iterator();
	        	sqlTemp += "(";
	        	while(it.hasNext()){
	        		String key =it.next();
	        		
	        		String value;
	        		value = map.get(key)!=null ?map.get(key).toString():"null";
	        		if(map.get(key)!=null)
	        			sqlTemp  += "'" + value + "'";
	        		
	        		else 
	        			sqlTemp +=  value ;
	        		
	        		if(it.hasNext()){
	        			sqlTemp += ",";
	        		}
	        	}
	        	
	        	sqlTemp += "),";
	        }
	        if(i != 0 ||list.size()==1){
	        	//��n��
	        	
	        	Map<String,Object> map=list.get(i);
	        	java.util.Iterator<String> it = map.keySet().iterator();
	        	
	        	sqlTemp += "(";
	        	while(it.hasNext()){
	        		String key =it.next();
	        		
	        		String value;
	        		
	        		value = map.get(key)!=null ?map.get(key).toString():"null";
	        		if(map.get(key)!=null)
	        			sqlTemp  += "'" + value + "'";
	        		else 
	        			sqlTemp  +=  value;
	        		
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
			long start = System.currentTimeMillis(); //����ʼ��¼ʱ��
			
			ExecutorService exec=Executors.newFixedThreadPool(102);
			//while(true){
			
			exec.execute(new initWx_subscriber(100));
			//exec.execute(new initWx_subscriber(1000));
			//System.out.println(start1-start);
			 
	        exec.shutdown();
			
	         
	        while(true){
	             if(exec.isTerminated()){
	                 //System.out.println("Finally do something ");
	                 long end = System.currentTimeMillis();
	                 System.out.println("��ʱ: " + (end - start) + "ms");
	                 
	                 
	                 break;
	             }

	         }
		
			
		}
			
		
	}

