package net.sp.init.combineSql;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import net.sp.init.mysql.mysqlInit;

import com.tonetime.commons.database.helper.DbHelper;
import com.tonetime.commons.database.helper.JdbcCallback;
public class sql implements Runnable{
	List<Map<String,Object>> list;
	int start;
	int end;
	int index;
	
	public sql(int index,List<Map<String,Object>>list , int s,int e){
		this.index=index;
		this.list=list;
		this.start=s;
		this.end=e;
		try {
			Thread.sleep(100);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		insert(combineSql( list, start, end));
	}
	 public String combineSql(List<Map<String,Object>> list,int start,int end){
		
		 long start1 =System.currentTimeMillis();
		 StringBuffer sqlTemp=new StringBuffer("INSERT INTO iov_track_"+index
	        		+ " VALUES ");
		 
		 for(int i = start ;i<end;i++){
			
			final Map<String,Object>	map=list.get(i);
			 
			sqlTemp.append("(");
			 
			
     		
	     	String value;
	     	/*
	     	 * n_id
	     	 */
	     	value = map.get("n_id")!=null ?map.get("n_id").toString():null;
	     	if( null != value){
	     		sqlTemp.append("'");
	     		sqlTemp.append(value);
	     		sqlTemp.append("' ,");
	     	}
	     	else {
	     		sqlTemp.append(value);
	     		sqlTemp.append(",");
	     	}
	     	
	     	value = map.get("c_model")!=null ?map.get("c_model").toString():null;
	     	if( null != value){
	     		sqlTemp.append("'");
	     		sqlTemp.append(value);
	     		sqlTemp.append("' ,");
	     	}
	     	else {
	     		sqlTemp.append(value);
	     		sqlTemp.append(",");
	     	}
	     	
	     	value = map.get("c_imei")!=null ?map.get("c_imei").toString(): null ;
	     	if( null != value){
	     		sqlTemp.append("'");
	     		sqlTemp.append( value );
	     		sqlTemp.append("' ,");
	     	}
	     	else {
	     		sqlTemp.append(value);
	     		sqlTemp.append(",");
	     	}
	     	
	     	
	     	
	     	value = map.get("n_type")!=null ?map.get("n_type").toString(): null ;
	     	if( null != value){
	     		sqlTemp.append("'");
	     		sqlTemp.append( value );
	     		sqlTemp.append("' ,");
	     	}
	     	else {
	     		sqlTemp.append(value);
	     		sqlTemp.append(",");
	     	}

	     	value = map.get("n_clng")!=null ?map.get("n_clng").toString(): null ;
	     	if( null != value){
	     		sqlTemp.append("'");
	     		sqlTemp.append( value );
	     		sqlTemp.append("' ,");
	     	}
	     	else {
	     		sqlTemp.append(value);
	     		sqlTemp.append(",");
	     	}

	     	value = map.get("n_clat")!=null ?map.get("n_clat").toString(): null ;
	     	if( null != value){
	     		sqlTemp.append("'");
	     		sqlTemp.append( value );
	     		sqlTemp.append("' ,");
	     	}
	     	else {
	     		sqlTemp.append(value);
	     		sqlTemp.append(",");
	     	}

	     	value = map.get("n_effect")!=null ?map.get("n_effect").toString(): null ;
	     	if( null != value){
	     		sqlTemp.append("'");
	     		sqlTemp.append( value );
	     		sqlTemp.append("' ,");
	     	}
	     	else {
	     		sqlTemp.append(value);
	     		sqlTemp.append(",");
	     	}

	     	value = map.get("n_lng")!=null ?map.get("n_lng").toString(): null ;
	     	if( null != value){
	     		sqlTemp.append("'");
	     		sqlTemp.append( value );
	     		sqlTemp.append("' ,");
	     	}
	     	else {
	     		sqlTemp.append(value);
	     		sqlTemp.append(",");
	     	}

	     	value = map.get("n_lat")!=null ?map.get("n_lat").toString(): null ;
	     	if( null != value){
	     		sqlTemp.append("'");
	     		sqlTemp.append( value );
	     		sqlTemp.append("' ,");
	     	}
	     	else {
	     		sqlTemp.append(value);
	     		sqlTemp.append(",");
	     	}

	     	value = map.get("n_satellite")!=null ?map.get("n_satellite").toString(): null ;
	     	if( null != value){
	     		sqlTemp.append("'");
	     		sqlTemp.append( value );
	     		sqlTemp.append("' ,");
	     	}
	     	else {
	     		sqlTemp.append(value);
	     		sqlTemp.append(",");
	     	}

	     	value = map.get("n_altitude")!=null ?map.get("n_altitude").toString(): null ;
	     	if( null != value){
	     		sqlTemp.append("'");
	     		sqlTemp.append( value );
	     		sqlTemp.append("' ,");
	     	}
	     	else {
	     		sqlTemp.append(value);
	     		sqlTemp.append(",");
	     	}

	     	value = map.get("n_accuracy")!=null ?map.get("n_accuracy").toString(): null ;
	     	if( null != value){
	     		sqlTemp.append("'");
	     		sqlTemp.append( value );
	     		sqlTemp.append("' ,");
	     	}
	     	else {
	     		sqlTemp.append(value);
	     		sqlTemp.append(",");
	     	}

	     	value = map.get("n_bearing")!=null ?map.get("n_bearing").toString(): null ;
	     	if( null != value){
	     		sqlTemp.append("'");
	     		sqlTemp.append( value );
	     		sqlTemp.append("' ,");
	     	}
	     	else {
	     		sqlTemp.append(value);
	     		sqlTemp.append(",");
	     	}

	     	value = map.get("n_speed")!=null ?map.get("n_speed").toString(): null ;
	     	if( null != value){
	     		sqlTemp.append("'");
	     		sqlTemp.append( value );
	     		sqlTemp.append("' ,");
	     	}
	     	else {
	     		sqlTemp.append(value);
	     		sqlTemp.append(",");
	     	}

	     	value = map.get("n_speed_2")!=null ?map.get("n_speed_2").toString(): null ;
	     	if( null != value){
	     		sqlTemp.append("'");
	     		sqlTemp.append( value );
	     		sqlTemp.append("' ,");
	     	}
	     	else {
	     		sqlTemp.append(value);
	     		sqlTemp.append(",");
	     	}

	     	value = map.get("n_distance")!=null ?map.get("n_distance").toString(): null ;
	     	if( null != value){
	     		sqlTemp.append("'");
	     		sqlTemp.append( value );
	     		sqlTemp.append("' ,");
	     	}
	     	else {
	     		sqlTemp.append(value);
	     		sqlTemp.append(",");
	     	}

	     	value = map.get("n_gpstime")!=null ?map.get("n_gpstime").toString(): null ;
	     	if( null != value){
	     		sqlTemp.append("'");
	     		sqlTemp.append( value );
	     		sqlTemp.append("' ,");
	     	}
	     	else {
	     		sqlTemp.append(value);
	     		sqlTemp.append(",");
	     	}

	     	value = map.get("c_token")!=null ?map.get("c_token").toString(): null ;
	     	if( null != value){
	     		sqlTemp.append("'");
	     		sqlTemp.append( value );
	     		sqlTemp.append("' ,");
	     	}
	     	else {
	     		sqlTemp.append(value);
	     		sqlTemp.append(",");
	     	}

	     	value = map.get("t_data_time")!=null ?map.get("t_data_time").toString():null;
	     	if( null != value){
	     		sqlTemp.append("'");
	     		sqlTemp.append( value );
	     		sqlTemp.append("' ,");
	     	}
	     	else {
	     		sqlTemp.append(value);
	     		sqlTemp.append(",");
	     	}

	     	value = map.get("t_create_time")!=null ?map.get("t_create_time").toString(): null ;
	     	
	     	if(i != (end-1) ){
	     	
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
	     
	     return sql_Insert;
		 
	 	
	}
	public synchronized void insert(final String sql){
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
	public static void main(String[] args) {
		Map<String,Object> params = new HashMap<String,Object>();
		params.put("a", 1);
		params.put("b", 1);
		params.put("c", 1);
		params.put("d", 1);
		params.put("e", 1);
		params.put("f", 1);
		long start = System.currentTimeMillis();
				
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
		for (int i = 0; i < 10000; i++) {
			list.add(params);
			
		}
		long start1 = System.currentTimeMillis();
		System.out.println("cost:"+(start1-start));
		
//		String sql = "";
		StringBuffer sql = new StringBuffer();
		for (Map<String, Object> map : list) {
//			sql += map.get("a")+","+map.get("b")+","+map.get("c")+","+map.get("d")+","+map.get("e")+","+map.get("f");
			
			sql.append(map.get("a")+","+map.get("b")+","+map.get("c")+","+map.get("d")+","+map.get("e")+","+map.get("f"));
		}
		long start2 = System.currentTimeMillis();
		System.out.println("cost con:"+(start2-start1));
	}
}
