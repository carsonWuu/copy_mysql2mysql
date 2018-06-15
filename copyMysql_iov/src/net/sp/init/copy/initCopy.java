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
	
	private int index;//表的下表
	private int Count;//一次分页的个数
	private Connection connM,connS ;
	public initCopy(int index,int count) throws SQLException{
		connM = new DBUtil().getMasterConnection();
		connM.setAutoCommit(false); 
		
		connS = new DBUtil().getSlaveConnection();
	
		this.index=index;
		this.Count=count;
	}
	
	@Override
	public void run(){
		int sum=initCopy.number(index);
		
		System.out.println("总数为："+sum);
		int count=0;
		
		try {
			while(count<sum){
				//System.out.println("次数");
				getPage(count,count+Count);
				
				count+=Count;
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		//System.out.println(count);
		
		
		try {
			connM.close();
			connS.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
	}
	public static int number(final int num){//总数量
		Map<String, Object> map=null;
		try {
			map=(Map<String, Object>) DbHelper.execute(mysqlInit.getInstance().getSlaveSource(), new JdbcCallback() {
				
				@Override
				public Object doInJdbc(Connection arg0) throws SQLException, Exception {
					// TODO Auto-generated method stub
					String sql="select n_id from iov_track_"+num+" order by n_id desc limit 1";
					return DbHelper.queryFor(arg0,sql);
				}
			});
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 Number value =0;
		 for (String key : map.keySet()) {
             value =  (Number)map.get(key);
             break;
		 }
		return (value.intValue());
		
	}
	
	
	
	public void getPage(final int start,final int end) throws SQLException{//分页查出数据元
		
		
		
		/*copy iov_track_0 to iov_track_99
		 * 
		 */
		String sql_Select="select * from iov_track_"+index+"  where n_id > "+ start+" and n_id <= "+end;
		String sql_Insert = "INSERT INTO iov_track_"+index+" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)";  
		
        
        
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
	                
	               prs.setInt(4, rs.getInt(4)); 
	               prs.setDouble(5, rs.getDouble(5));
	               prs.setDouble(6,rs.getDouble(6));
	                
	                
	               prs.setInt(7, rs.getInt(7));  
	               prs.setDouble(8, rs.getDouble(8));
	               prs.setDouble(9, rs.getDouble(9));
	               prs.setInt(10, rs.getInt(10));  
	               prs.setDouble(11, rs.getDouble(11));
	                
	               prs.setFloat(12, rs.getFloat(12));
	               prs.setFloat(13, rs.getFloat(13));
	               prs.setDouble(14, rs.getDouble(14));
	               prs.setDouble(15, rs.getDouble(15));
	               prs.setDouble(16, rs.getDouble(16));
	               prs.setDouble(17, rs.getDouble(17));
	                
	               prs.setString(18,rs.getString(18));
	               prs.setTimestamp(19, rs.getTimestamp(19));
	               prs.setTimestamp(20, rs.getTimestamp(20));
	                
	               prs.addBatch();
                
               
                if (i++ % Count == 0) {
                	
                    prs.executeBatch();
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
		for(int i=0;i<100;i++){
			exec.execute(new initCopy(i,1000));
		}
//		exec.execute(new initWx_sub_dev(1000));
//		exec.execute(new initWx_subscriber(1000));
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
