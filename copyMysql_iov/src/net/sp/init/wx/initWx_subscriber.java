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




import net.sp.init.mysql.mysqlInit;

import com.tonetime.commons.database.DataSourceBuilder;
import com.tonetime.commons.database.helper.DbHelper;
import com.tonetime.commons.database.helper.JdbcCallback;

import net.sp.init.wx.*;


	public class initWx_subscriber implements Runnable{
		
		
		private int Count;//一次分页的个数
		
		public initWx_subscriber(int count) {
			
			this.Count=count;
		}
		
		@Override
		public void run(){
			int slaveMax=SlaveMax();
			int MasterMax=MasterMax();
			/*副表若为空则返回0
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
			 if(map==null)return 0;
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
			 if(map==null)return 0;
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
		public void InsertTable(final int start,final int end) throws SQLException{//分页查出数据元
			
			
			
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
			}
	        catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        
	        if(list==null||list.size()==0){
	        	return ;
	        }
	       
	        StringBuffer sqlTemp=new StringBuffer("INSERT INTO wx_subscriber VALUES");
	       
	        for(int i = 0 ;i<list.size();i++){
				
				final Map<String,Object>	map=list.get(i);
				 
				sqlTemp.append("(");
				 
				
	     		
		     	String value;
		     	/*
		     	 * n_id
		     	 */
		     	value = null!= map.get("n_id") ?map.get("n_id").toString():null;
		     	if( null != value){
		     		sqlTemp.append("'");
		     		sqlTemp.append(value);
		     		sqlTemp.append("' ,");
		     	}
		     	else {
		     		sqlTemp.append(value);
		     		sqlTemp.append(",");
		     	}
		     	
		     	value = null!=  map.get("c_app_code") ?map.get("c_app_code").toString():null;
		     	if( null != value){
		     		sqlTemp.append("'");
		     		sqlTemp.append(value);
		     		sqlTemp.append("' ,");
		     	}
		     	else {
		     		sqlTemp.append(value);
		     		sqlTemp.append(",");
		     	}
		     	
		     	value = null!=  map.get("c_open_id") ?map.get("c_open_id").toString(): null ;
		     	if( null != value){
		     		sqlTemp.append("'");
		     		sqlTemp.append( value );
		     		sqlTemp.append("' ,");
		     	}
		     	else {
		     		sqlTemp.append(value);
		     		sqlTemp.append(",");
		     	}
		     	
		     	
		     	
		     	value = null!=  map.get("c_union_id") ?map.get("c_union_id").toString(): null ;
		     	if( null != value){
		     		sqlTemp.append("'");
		     		sqlTemp.append( value );
		     		sqlTemp.append("' ,");
		     	}
		     	else {
		     		sqlTemp.append(value);
		     		sqlTemp.append(",");
		     	}

		     	value = null!=  map.get("c_wx_num") ?map.get("c_wx_num").toString(): null ;
		     	if( null != value){
		     		sqlTemp.append("'");
		     		sqlTemp.append( value );
		     		sqlTemp.append("' ,");
		     	}
		     	else {
		     		sqlTemp.append(value);
		     		sqlTemp.append(",");
		     	}

		     	value = null!=  map.get("c_wx_nickname") ?map.get("c_wx_nickname").toString(): null ;
		     	if( null != value){
		     		
		     		if(value.contains("'"))value=value.replace("'", "\\\'");
		     		System.out.println(value);
		     		sqlTemp.append("'");
		     		sqlTemp.append( value );
		     		sqlTemp.append("' ,");
		     	}
		     	else {
		     		sqlTemp.append(value);
		     		sqlTemp.append(",");
		     	}

		     	value = null!=  map.get("c_mobile") ?map.get("c_mobile").toString(): null ;
		     	if( null != value){
		     		sqlTemp.append("'");
		     		sqlTemp.append( value );
		     		sqlTemp.append("' ,");
		     	}
		     	else {
		     		sqlTemp.append(value);
		     		sqlTemp.append(",");
		     	}

		     	

		     	value = null!=  map.get("n_sex") ?map.get("n_sex").toString(): null ;
		     	if( null != value){
		     		sqlTemp.append("'");
		     		sqlTemp.append( value );
		     		sqlTemp.append("' ,");
		     	}
		     	else {
		     		sqlTemp.append(value);
		     		sqlTemp.append(",");
		     	}

		     	value = null!=  map.get("c_city") ?map.get("c_city").toString(): null ;
		     	if( null != value){
		     		sqlTemp.append("'");
		     		sqlTemp.append( value );
		     		sqlTemp.append("' ,");
		     	}
		     	else {
		     		sqlTemp.append(value);
		     		sqlTemp.append(",");
		     	}

		     	value = null!=  map.get("c_avatar") ?map.get("c_avatar").toString(): null ;
		     	if( null != value){
		     		sqlTemp.append("'");
		     		sqlTemp.append( value );
		     		sqlTemp.append("' ,");
		     	}
		     	else {
		     		sqlTemp.append(value);
		     		sqlTemp.append(",");
		     	}

		     	value = null!=  map.get("c_country") ?map.get("c_country").toString(): null ;
		     	if( null != value){
		     		sqlTemp.append("'");
		     		sqlTemp.append( value );
		     		sqlTemp.append("' ,");
		     	}
		     	else {
		     		sqlTemp.append(value);
		     		sqlTemp.append(",");
		     	}

		     	value = null!=  map.get("c_province") ?map.get("c_province").toString(): null ;
		     	if( null != value){
		     		sqlTemp.append("'");
		     		sqlTemp.append( value );
		     		sqlTemp.append("' ,");
		     	}
		     	else {
		     		sqlTemp.append(value);
		     		sqlTemp.append(",");
		     	}

		     	value = null!=  map.get("t_sub_time") ?map.get("t_sub_time").toString(): null ;
		     	if( null != value){
		     		sqlTemp.append("'");
		     		sqlTemp.append( value );
		     		sqlTemp.append("' ,");
		     	}
		     	else {
		     		sqlTemp.append(value);
		     		sqlTemp.append(",");
		     	}

		     	value = null!=  map.get("n_is_sub") ?map.get("n_is_sub").toString(): null ;
		     	if( null != value){
		     		sqlTemp.append("'");
		     		sqlTemp.append( value );
		     		sqlTemp.append("' ,");
		     	}
		     	else {
		     		sqlTemp.append(value);
		     		sqlTemp.append(",");
		     	}
		     	
		     	value = null!=  map.get("n_lng") ?map.get("n_lng").toString(): null ;
		     	if( null != value){
		     		sqlTemp.append("'");
		     		sqlTemp.append( value );
		     		sqlTemp.append("' ,");
		     	}
		     	else {
		     		sqlTemp.append(value);
		     		sqlTemp.append(",");
		     	}

		     	value = null!=  map.get("n_lat") ?map.get("n_lat").toString(): null ;
		     	if( null != value){
		     		sqlTemp.append("'");
		     		sqlTemp.append( value );
		     		sqlTemp.append("' ,");
		     	}
		     	else {
		     		sqlTemp.append(value);
		     		sqlTemp.append(",");
		     	}
		     	value = null!=  map.get("n_precision") ?map.get("n_precision").toString(): null ;
		     	if( null != value){
		     		sqlTemp.append("'");
		     		sqlTemp.append( value );
		     		sqlTemp.append("' ,");
		     	}
		     	else {
		     		sqlTemp.append(value);
		     		sqlTemp.append(",");
		     	}

		     	value = null!=  map.get("c_lac_label") ?map.get("c_lac_label").toString(): null ;
		     	if( null != value){
		     		sqlTemp.append("'");
		     		sqlTemp.append( value );
		     		sqlTemp.append("' ,");
		     	}
		     	else {
		     		sqlTemp.append(value);
		     		sqlTemp.append(",");
		     	}

		     	value = null!=  map.get("t_unsub_time") ?map.get("t_unsub_time").toString():null;
		     	if( null != value){
		     		sqlTemp.append("'");
		     		sqlTemp.append( value );
		     		sqlTemp.append("' ,");
		     	}
		     	else {
		     		sqlTemp.append(value);
		     		sqlTemp.append(",");
		     	}
		     	
		     	value = null!=  map.get("t_update_time") ?map.get("t_update_time").toString():null;
		     	if( null != value){
		     		sqlTemp.append("'");
		     		sqlTemp.append( value );
		     		sqlTemp.append("' ,");
		     	}
		     	else {
		     		sqlTemp.append(value);
		     		sqlTemp.append(",");
		     	}
		     	
		     	value = null!= map.get("t_create_time") ?map.get("t_create_time").toString(): null ;
		     	
		     	if(i != (list.size()-1) ){
		     	
			     	if( null != value){
			     		sqlTemp.append("'");
			     		sqlTemp.append( value );
			     		sqlTemp.append("' ),");
			     	}
			     	else {
			     		sqlTemp.append(value);
			     		sqlTemp.append("),");
			     	}
		     	}
		     	else {
		     		if( null != value){
			     		sqlTemp.append("'");
			     		sqlTemp.append( value );
			     		sqlTemp.append("' )");
			     	}
			     	else {
			     		sqlTemp.append(value);
			     		sqlTemp.append(")");
			     	}
		     	}
				 
			 }
			
	    
	     /*
	      * 2.Insert
	      */
	     
		     final String sql_Insert =sqlTemp.toString(); 
		     long end1 =System.currentTimeMillis();
		     
		     //System.out.println("time:"+(end1-start1));
		     
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
			
			exec.execute(new initWx_subscriber(100));
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

