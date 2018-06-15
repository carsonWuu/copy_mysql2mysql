package net.sp.init.mysql;

import javax.sql.DataSource;

public class mysqlInit {
	private com.tonetime.commons.database.Configuration configure   = null;
	private com.tonetime.commons.database.DataSourceBuilder builder = null;
	
	DataSource masterSource=null;
	DataSource slaveSource=null;
	private mysqlInit(){
		configure = new com.tonetime.commons.database.Configuration("db.properties");
		builder   = com.tonetime.commons.database.DataSourceBuilder.builder(configure);
		
		masterSource=builder.getDataSource("dbMaster");
		slaveSource =builder.getDataSource("dbSlave");
		
	}
	
	private static class HolderSingleton{
		static mysqlInit instance = new mysqlInit();
	}
	
	public static mysqlInit getInstance(){
		return HolderSingleton.instance;
	}
	
	public DataSource getMasterSource(){
		return masterSource;
	}
	public DataSource getSlaveSource(){
		return slaveSource;
	}
}
