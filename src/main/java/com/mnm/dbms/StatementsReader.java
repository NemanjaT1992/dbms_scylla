package com.mnm.dbms;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.mnm.dbms.ScyllaAdapter;

public class StatementsReader {
	
	int numOfTables = 0;
	int numOfExecuted = 0;

	public StatementsReader() {

	}

	public void read(String rootFolder) {
		createTables(rootFolder);
		insertValues(rootFolder);
	}

	public void createTables(String rootFolder) 
	{
		File root = new File(rootFolder);
		File[] listOfFiles = root.listFiles();

		for (File file : listOfFiles) {
			if (file.isFile() && file.getName().contains("create")) {
				String filePath = rootFolder + "/" + file.getName();
				try {
					BufferedReader br = new BufferedReader(new FileReader(filePath));

					StringBuilder stringBuilder = new StringBuilder();
					try {
						String line = br.readLine();

						while (line != null) {
							line = line.trim();
							stringBuilder.append(line + " ");
							line = br.readLine();
						}
					} catch (IOException e) {
						e.printStackTrace();
					}

					String query = stringBuilder.toString();

					query = query.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
					// System.out.println(query);
					System.out.println("creating table " + rootFolder);
					ScyllaAdapter.getInstance().executeQuery(query);
					System.out.println("table created");
					numOfTables++;
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
			} else if (file.isFile())
				continue;
			else
				createTables(rootFolder + "/" + file.getName());
		}
	}

	public void insertValues(String rootFolder) 
	{	
		File root = new File(rootFolder);
		File[] listOfFiles = root.listFiles();

		for (File file : listOfFiles) 
		{
			if (file.isFile() && file.getName().contains("insert")) 
			{
//				if(!rootFolder.equals("InstructionsFolder/contents"))
//					continue;
//				
				String filePath = rootFolder + "/" + file.getName();

				try {
					BufferedReader br = new BufferedReader(new FileReader(filePath));

					StringBuilder stringBuilder = new StringBuilder();
					
					try {
						System.out.println("populating table " + rootFolder);
						
						String line = br.readLine();
						int counter = 1;
						while (true) 
						{	
							if(counter == 25 || line == null)
							{
								if(line == null)
									break;
								
								stringBuilder.insert(0, "BEGIN BATCH\n");
								stringBuilder.append(" APPLY BATCH;");
								
								String query = stringBuilder.toString();
//								System.out.println(query);
								
//								if(rootFolder.equals("InstructionsFolder/contents"))
//									System.out.println(query);
								
								ScyllaAdapter.getInstance().executeQuery(query);
								
								counter = 0;
								stringBuilder.setLength(0);
							}
							
							
							line = line.trim();
							stringBuilder.append(line);
							line = br.readLine();
							counter++;
						}
					} catch (IOException e) {
						e.printStackTrace();
					}

					System.out.println("(" + (++numOfExecuted) + "/" + numOfTables + ") table " + rootFolder + " populated");

				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
			} else if (file.isFile())
				continue;
			else
				insertValues(rootFolder + "/" + file.getName());
		}
	}
}
