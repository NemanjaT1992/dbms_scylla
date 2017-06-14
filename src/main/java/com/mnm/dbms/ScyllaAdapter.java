package com.mnm.dbms;

import java.net.InetSocketAddress;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class ScyllaAdapter 
{
	private static ScyllaAdapter instance = null;
	String keyspace = "dbms_keyspace";
	Session session;
	
	Cluster cluster = new Cluster.Builder()
			.addContactPointsWithPorts(
					new InetSocketAddress("192.168.1.8", 9042)//,
//					new InetSocketAddress("192.168.1.9", 9042),
//					new InetSocketAddress("192.168.1.10", 9042)
					//new InetSocketAddress("192.168.1.6", 9042)
					)
		    .build();
	
	
	public ScyllaAdapter()
	{

	}
	
	public boolean connect()
	{
		session = cluster.connect(keyspace);
		return session.isClosed();
	}
	
	public boolean disconnect()
	{
		session.close();
		cluster.close();
		
		return session.isClosed() && cluster.isClosed();
	}
	
	public static ScyllaAdapter getInstance()
	{
		if(instance == null)
			instance = new ScyllaAdapter();
		return instance;
	}
	
	public boolean executeQuery(String query)
	{
		ResultSet result = session.execute(query);
		
		return result.wasApplied();
	}
	
	public void deleteAllTablesInKeyspace(String kesypace)
	{
		String selectQuery = "select columnfamily_name from system.schema_columnfamilies where keyspace_name = '" + keyspace + "';";
		ResultSet result = session.execute(selectQuery);
		
		for (Row row : result) 
		{
			String dropQuery = "DROP TABLE " + row.getString("columnfamily_name");
			session.execute(dropQuery);
		}
	}
	
	public void createKeyspace(String keyspaceName)
	{
		String dropKeyspaceQuery = "DROP KEYSPACE IF EXISTS " + keyspaceName;
		ResultSet dropKeyspaceResult = session.execute(dropKeyspaceQuery);
		
		if(dropKeyspaceResult.wasApplied())
			System.out.println("********	keyspace " + keyspaceName + "	DROPED");
		else
			System.out.println("********	ERROR DROPING KEYSPACE " + keyspaceName);
		
		String createKeyspaceQuery = "CREATE KEYSPACE " + keyspaceName + " WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': 3} AND durable_writes = true;";
		ResultSet createKeyspaceResult = session.execute(createKeyspaceQuery);
		
		if(createKeyspaceResult.wasApplied())
			System.out.println("********	keyspace " + keyspaceName + "	CREATED");
		else
			System.out.println("********	ERROR CREATING KEYSPACE " + keyspaceName);
	}
}



















