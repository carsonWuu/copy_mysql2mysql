package net.sp.init.copy;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.swing.text.html.HTMLDocument.Iterator;

import net.sp.init.mysql.DBUtil;
import net.sp.init.mysql.mysqlInit;







import com.mysql.jdbc.PreparedStatement;
import com.tonetime.commons.database.helper.DbHelper;
import com.tonetime.commons.database.helper.JdbcCallback;

import net.sp.init.wx.*;
public class initCopy implements Runnable{
	
	private int Count;//一次分页的个数
	private int index;
	public initCopy(int index,int count) {
		this.index=index;
		this.Count=count;
	}
	
	@Override
	public void run(){
		int slaveMax=SlaveMax();
		int MasterMax=MasterMax();
		/*副表若为空则返回-1
		 * 当副表最大值小于主表最大值时，则进行拷贝，完全拷贝
		 */
		
		System.out.println(slaveMax+"==>"+MasterMax);
		if(slaveMax>MasterMax){
			//table has changed!
			// do delete then insert all element
			
			
				int count=MasterMax;
				
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
		else{
			System.out.println("iov_track_"+index+" no change!");
		}
		
			
	}
	public  int SlaveMax(){
		//the biggest n_id of SlaveSource
		Map<String, Object> map=null;
		try {
			map=(Map<String, Object>) DbHelper.execute(mysqlInit.getInstance().getSlaveSource(), new JdbcCallback() {
				
				@Override
				public Object doInJdbc(Connection arg0) throws SQLException, Exception {
					// TODO Auto-generated method stub
					String sql="select n_id from iov_track_"+index+" order by n_id desc limit 1";
					
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
	
	public int MasterMax(){
		//the biggest n_id of MasterSource
		
		Map<String, Object> map=null;
		try {
			map=(Map<String, Object>) DbHelper.execute(mysqlInit.getInstance().getMasterSource(), new JdbcCallback() {
				
				@Override
				public Object doInJdbc(Connection arg0) throws SQLException, Exception {
					// TODO Auto-generated method stub
					String sql="select n_id from iov_track_"+index+" order by n_id desc limit 1";
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
		final String sql_Delete = "truncate  iov_track_"+index;  
		
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
		
		final String sql_Select="select * from iov_track_"+index+"  where n_id > "+ start+" and n_id <= "+end;
		
		
        
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
       
        String sqlTemp="INSERT INTO iov_track_"+index+"(n_id,t_data_time,n_distance,t_create_time,n_bearing,n_accuracy,n_lng,n_lat,n_speed_2,n_gpstime,c_token,n_speed,n_clng,c_model,n_effect,n_type,n_clat,n_altitude,c_imei,n_satellite) "
        		+ "VALUES ";
        int i=0;
        for(;i<list.size()-1;i++){
        	//第n-1个
        	Map<String,Object> map=list.get(i);
        	java.util.Iterator<String> it = map.keySet().iterator();
        	sqlTemp += "(";
        	while(it.hasNext()){
        		String key =it.next();
        		
        		String value;
        		value = map.get(key)!=null ?map.get(key).toString():"null";
        		if(map.get(key)!=null)sqlTemp  += "'" + value + "'";
        		else sqlTemp  +=  value;
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
        		value = map.get(key)!=null ?map.get(key).toString():"null";
        		if(map.get(key)!=null)sqlTemp  += "'" + value + "'";
        		else sqlTemp  +=  value;
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
        
        //System.out.println(sqlTemp);
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
		for(int i=0;i<1;i++){
			exec.execute(new initCopy(i,1000));
			
		}
		
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
