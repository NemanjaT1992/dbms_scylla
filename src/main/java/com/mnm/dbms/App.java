package com.mnm.dbms;
import org.apache.log4j.BasicConfigurator;

import com.mnm.dbms.ScyllaAdapter;
import com.mnm.dbms.StatementsReader;
/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	BasicConfigurator.configure();
    	
    	ScyllaAdapter.getInstance().connect();
//    	ScyllaAdapter.getInstance().createKeyspace("dbms_keyspace");
    	ScyllaAdapter.getInstance().deleteAllTablesInKeyspace("dbms_keyspace");
    	
    	StatementsReader reader = new StatementsReader();
    	reader.read("InstructionsFolder");
    	
    	ScyllaAdapter.getInstance().disconnect();
    	
        System.out.println( "Application closed!");
    }
}
