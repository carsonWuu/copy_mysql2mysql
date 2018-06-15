package net.sp.init.wx;

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
	public class initWx_subscriber implements Runnable{
		
		
		private int Count;//一次分页的个数
		private Connection connM,connS ;
		public initWx_subscriber(int count) throws SQLException{
			connM = new DBUtil().getMasterConnection();
			connM.setAutoCommit(false); 
			
			connS = new DBUtil().getSlaveConnection();
		
			
			this.Count=count;
		}
		
		@Override
		public void run(){
			int slaveMax=SlaveMax();
			int MasterMax=MasterMax();
			/*副表若为空则返回-1
			 * 当副表最大值小于主表最大值时，则进行拷贝，完全拷贝
			 */
			System.out.println(slaveMax+"\t"+MasterMax);
			if(slaveMax>MasterMax){
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
			
			
			
			try {
				connM.close();
				connS.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
				
		}
		public static int SlaveMax(){//总数量
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
		
		public static int MasterMax(){//总数量
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
			String sql_Delete = "truncate  wx_subscriber";  
			
	        PreparedStatement jps=null;
			try {
				jps = (PreparedStatement) connM.prepareStatement(sql_Delete, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);//**********删除Master表数据!!!
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.err.println("删除语句有误");
				return false;
			}
	            
	        try {
				ResultSet rs = jps.executeQuery();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.err.println("删除语句执行有误");
				return false;
			}
	        return true;
		}
		public void InsertTable(final int start,final int end) throws SQLException{//分页查出数据元
			
			
			
			/*copy iov_track_0 to iov_track_99
			 * 
			 */
			String sql_Select="select * from wx_subscriber  where n_id > "+ start+" and n_id <= "+end;
			String sql_Insert = "INSERT INTO wx_subscriber VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?)";  
			
	        
	        
	        int i = 1;
	        
	        PreparedStatement jps = (PreparedStatement) connS.prepareStatement(sql_Select, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
	            
	        ResultSet rs = jps.executeQuery();
	        if (!rs.next()) return;
	        PreparedStatement prs = (PreparedStatement) connM.prepareStatement(sql_Insert);
	        do {
	            	//System.out.println(rs.getInt(1));
	            	   prs.setInt(1, rs.getInt(1));  
	            	   
		               prs.setString(2,rs.getString(2));  
		               prs.setString(3,rs.getString(3));
		               prs.setString(4,rs.getString(4));  
		               prs.setString(5,rs.getString(5));
		               prs.setString(6,rs.getString(6));  
		               prs.setString(7,rs.getString(7));
		               
		               prs.setInt(8, rs.getInt(8)); 
		               
		               prs.setString(9,rs.getString(9));  
		               prs.setString(10,rs.getString(10));
		               prs.setString(11,rs.getString(11));  
		               prs.setString(12,rs.getString(12));
		               
		               prs.setTimestamp(13, rs.getTimestamp(13));
		               
		               prs.setInt(14, rs.getInt(14)); 
		               prs.setDouble(15, rs.getDouble(15));
		               prs.setDouble(16, rs.getDouble(16));
		               prs.setDouble(17, rs.getDouble(17));
		               prs.setString(18,rs.getString(18));
		               
		               prs.setTimestamp(19, rs.getTimestamp(19));
		               prs.setTimestamp(20, rs.getTimestamp(20));
		               prs.setTimestamp(21, rs.getTimestamp(21));
		               //System.out.println(rs.getString(6));
		              
		                
		               prs.addBatch();
	                
	               
	                if (i++ % Count == 0) {
	                	System.out.println(i);
	                    prs.executeBatch();
	                    connM.commit();
	                    prs.clearBatch();
	                }
	            } while (rs.next());
	            prs.executeBatch();
	            rs.close();
	            
	       
	        connM.commit();
	        connM.rollback(); 
	        
	    }
			

		
		public static void main(String []args) throws Exception{
			//initCopy.number(0);
			System.out.println("start!");
			long start = System.currentTimeMillis(); //程序开始记录时间
			
			ExecutorService exec=Executors.newFixedThreadPool(102);
			//while(true){
			
			//exec.execute(new initWx_sub_dev(1000));
			exec.execute(new initWx_subscriber(1000));
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

