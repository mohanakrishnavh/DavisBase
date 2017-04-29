package database;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.Scanner;

/**
 *
 * @author Mohanakrishna
 */

public class DavisBase {

	static String prompt = "davisql> ";
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");

	static String currentDB = "user_data";
	
	public static void main(String[] args) {
		initializeSystemDatabase();

		// createDirectory("emkae");
		System.out.println(
				"************************************************************************************************************************");
		System.out.println("Welcome to DavisBase Version 1.0");
		System.out.println("Type \"help;\" to view all the commands supported by DavisBase");
		System.out.println(
				"************************************************************************************************************************");

		/* Variable to collect user input from the prompt */
		String query = "";

		while (!query.equals("exit")) 
		{
			System.out.print(prompt);
			/* toLowerCase() renders command case insensitive */
			query = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			parsequery(query);
		}
		System.out.println("Exiting from DavisBase");

	}

	public static void initializeSystemDatabase() 
	{
		try {
			File data = new File("data");//\\catalog");
			if (!data.exists()) {
				data.mkdir();
			}
			File dbCatalog = new File("data\\catalog");
			if (dbCatalog.mkdir()) {
				System.out.println("System directory 'data\\catalog' doesn't exit, Initializing catalog!");
				Table.initializeDataStore();
			} else {
				boolean catalog = false;
				String meta_columns = "davisbase_columns.tbl";
				String meta_tables = "davisbase_tables.tbl";
				String[] tableList = dbCatalog.list();

				for (int i = 0; i < tableList.length; i++) {
					if (tableList[i].equals(meta_columns))
						catalog = true;
				}
				if (!catalog) {
					System.out.println(
							"System table 'davisbase_columns.tbl' does not exit, initializing davisbase_columns");
					System.out.println();
					Table.initializeDataStore();
				}
				catalog = false;
				for (int i = 0; i < tableList.length; i++) {
					if (tableList[i].equals(meta_tables))
						catalog = true;
				}
				if (!catalog) {
					System.out.println(
							"System table 'davisbase_tables.tbl' does not exit, initializing davisbase_tables");
					System.out.println();
					Table.initializeDataStore();
				}
			}
		} catch (SecurityException se) {
			System.out.println("Catalog files not careated " + se);

		}

	}

	// Check if the table exists
	public static boolean tableExist(String table) {
		boolean table_check = false;
		//table = table + ".tbl";
		try {
			File user_tables = new File("data\\"+currentDB);
			if (user_tables.mkdir()) {
				System.out.println("System directory 'data\\user_data' doesn't exit, Initializing user_data!");
				//Table.initializeDataStore();
			}
			String[] tableList;
			tableList = user_tables.list();
			for (int i = 0; i < tableList.length; i++) {
				if (tableList[i].equals(table))
					return true;
			}
		} catch (SecurityException se) {
			System.out.println("Unable to create data container directory" + se);
		}

		return table_check;
	}

	public static String[] parserEquation(String equ) 
	{
		String cmp[] = new String[3];
		String temp[] = new String[2];
		if (equ.contains("=")) {
			temp = equ.split("=");
			cmp[0] = temp[0].trim();
			cmp[1] = "=";
			cmp[2] = temp[1].trim();
		}

		if (equ.contains(">")) {
			temp = equ.split(">");
			cmp[0] = temp[0].trim();
			cmp[1] = ">";
			cmp[2] = temp[1].trim();
		}

		if (equ.contains("<")) {
			temp = equ.split("<");
			cmp[0] = temp[0].trim();
			cmp[1] = "<";
			cmp[2] = temp[1].trim();
		}

		if (equ.contains(">=")) {
			temp = equ.split(">=");
			cmp[0] = temp[0].trim();
			cmp[1] = ">=";
			cmp[2] = temp[1].trim();
		}

		if (equ.contains("<=")) {
			temp = equ.split("<=");
			cmp[0] = temp[0].trim();
			cmp[1] = "<=";
			cmp[2] = temp[1].trim();
		}

		if (equ.contains("<>")) {
			temp = equ.split("<>");
			cmp[0] = temp[0].trim();
			cmp[1] = "<>";
			cmp[2] = temp[1].trim();
		}

		return cmp;
	}

	public static void parsequery(String query) 
	{

		String[] queryTokens = query.split(" ");

		switch (queryTokens[0]) 
		{
		
		case "use":
			if(queryTokens[1].equals("")){
				
				System.out.println("Incorrect input. Please check the help section to know the supported commands");
			}
			else{
				if(!Table.checkDB(queryTokens[1])){
					System.out.println("Database doesn't exist");
					System.out.println();
					break;
				}
				currentDB=queryTokens[1];
				System.out.println("using "+currentDB);
			}
			break;
		case "create":
			if(queryTokens[1].equals("table")){
				String createTable = queryTokens[2];
	
				String[] create_temp = query.split(createTable);
	
				String col_temp = create_temp[1].trim();
				String[] create_cols = col_temp.substring(1, col_temp.length() - 1).split(",");
				for (int i = 0; i < create_cols.length; i++)
					create_cols[i] = create_cols[i].trim();
				if (tableExist(createTable)) 
				{
					System.out.println("Table " + createTable + " already exists.");
					System.out.println();
					break;
				}
				Table.createTable(createTable, create_cols);
				System.out.println("Table "+createTable+" created successfully.");
				
			}
			else if(queryTokens[1].equals("database")){
				String createDB = queryTokens[2];
			
				Table.createDB(createDB);
				
			}
			else{
				System.out.println("Incorrect input. Please check the help section to know the supported commands");				
			}
			
			break;

		case "insert":
			String insert_table = queryTokens[2];
			String insert_vals = query.split("values")[1].trim();
			insert_vals = insert_vals.substring(1, insert_vals.length() - 1);
			String[] insert_values = insert_vals.split(",");
			for (int i = 0; i < insert_values.length; i++)
				insert_values[i] = insert_values[i].trim();
			if (!tableExist(insert_table)) {
				System.out.println("Table " + insert_table + " does not exist.");
				System.out.println();
				break;
			}
			RandomAccessFile file;
			try {
				file = new RandomAccessFile("data\\"+currentDB+"\\"+insert_table+"\\"+insert_table+".tbl", "rw");
				Table.insertInto(file,insert_table, insert_values);
			} catch (FileNotFoundException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			break;
			
		case "select":
			String[] select_cmp;
			String[] select_column;
			String[] select_temp = query.split("where");
			String[] selectQuery = select_temp[0].split("from");
			String selectTable = selectQuery[1].trim();
			String selectColumns = selectQuery[0].replace("select", "").trim();
			
			if(selectTable.equals("davisbase_tables"))
			{
				if (selectColumns.contains("*")) 
				{
					select_column = new String[1];
					select_column[0] = "*";
				} else 
				{
					select_column = selectColumns.split(",");
					for (int i = 0; i < select_column.length; i++)
						select_column[i] = select_column[i].trim();
				}
				if (select_temp.length > 1) {
					String filter = select_temp[1].trim();
					select_cmp = parserEquation(filter);
				} else {
					select_cmp = new String[0];
				}
				Table.select("data\\catalog\\davisbase_tables.tbl", selectTable, select_column, select_cmp);
				System.out.println();
				break;
			}
			
			else if(selectTable.equals("davisbase_columns"))
			{
				if (selectColumns.contains("*")) 
				{
					select_column = new String[1];
					select_column[0] = "*";
				} else 
				{
					select_column = selectColumns.split(",");
					for (int i = 0; i < select_column.length; i++)
						select_column[i] = select_column[i].trim();
				}
				if (select_temp.length > 1) {
					String filter = select_temp[1].trim();
					select_cmp = parserEquation(filter);
				} else {
					select_cmp = new String[0];
				}
				Table.select("data\\catalog\\davisbase_columns.tbl", selectTable, select_column, select_cmp);
				System.out.println();
				break;
			}

			else{
				if(!tableExist(selectTable)) {
		
					System.out.println("Table " + selectTable + " doesn't exist.");
					System.out.println("Please enter the correct table name.");
					System.out.println();
					break;
				}
			}

			if (select_temp.length > 1) 
			{
				String filter = select_temp[1].trim();
				select_cmp = parserEquation(filter);
			} else {
				select_cmp = new String[0];
			}

			if (selectColumns.contains("*")) {
				select_column = new String[1];
				select_column[0] = "*";
			} else {
				select_column = selectColumns.split(",");
				for (int i = 0; i < select_column.length; i++)
					select_column[i] = select_column[i].trim();
			}
			
			Table.select("data\\"+currentDB+"\\"+selectTable+"\\"+selectTable+".tbl", selectTable, select_column, select_cmp);
			System.out.println();
			break;	
			
		case "drop":
			if(queryTokens[1].equals("table")){
				String dropTable = queryTokens[2];
				if (!tableExist(dropTable)) 
				{
					System.out.println("Table " + dropTable + " does not exist.");
					System.out.println();
					break;
				}
				Table.drop(dropTable,currentDB);
				System.out.println("Table "+dropTable+" dropped successfully.");
			}
			else if(queryTokens[1].equals("database")){
				String dropDB = queryTokens[2];
				if (!Table.checkDB(dropDB)) 
				{
					System.out.println("Database " + dropDB + " does not exist.");
					System.out.println();
					break;
				}
				Table.dropDB(dropDB);
				System.out.println("Database "+dropDB+" dropped successfully.");
			}
			else{
				System.out.println("Incorrect input. Please check the help section to know the supported commands");
			}
			System.out.println();
			break;

		case "show":
			String cmd = queryTokens[1];
			System.out.println();
			if(cmd.equals("tables")){
				Table.show();
			}
			else if(cmd.equals("databases")){
				Table.showDB();
			}
			System.out.println();
			break;

		case "delete":
			String[] delete_cmp = null;
			String[] delete_temp = query.split("where");
			String[] deleteQuery = delete_temp[0].split("from");
			String deleteTable = deleteQuery[1].trim();
			if(!tableExist(deleteTable)) {
				
				System.out.println("Table " + deleteTable + " doesn't exist.");
				System.out.println("Please enter the correct table name.");
				System.out.println();
				break;
			}
			
			if (delete_temp.length > 1) {
				String filter = delete_temp[1].trim();  
				delete_cmp = parserEquation(filter);
			} else {
				delete_cmp = new String[0];
			}
			Table.delete(deleteTable, delete_cmp);
			System.out.println();
			break;
		
		case "update":
			String updateTable = queryTokens[1];
			String[] update_temp1 = query.split("set");
			String[] update_temp2 = update_temp1[1].split("where");
			String update_cmp_s = update_temp2[1];
			String update_set_s = update_temp2[0];
			String[] set = parserEquation(update_set_s);
			String[] update_cmp = parserEquation(update_cmp_s);
			if (!tableExist(updateTable)) 
			{
				System.out.println("Table " + updateTable + " does not exist.");
				System.out.println();
				break;
			}
			Table.update(updateTable, set, update_cmp);
			System.out.println("Table "+updateTable+" updated successfully.");
			System.out.println();
			break;

		case "help":
			System.out.println();
			System.out.println("List of all DavisBase commands:");
			System.out.println("1.DDL Commands:");
			System.out.println(
					"\t(a)SHOW TABLES;                                                   Displays a list of all tables in DavisBase");
			System.out.println(
					"\t(b)CREATE TABLE <table_name>;                                     Creates a new table schema, i.e. a new empty table");
			System.out.println(
					"\t(c)DROP TABLE <table_name>;                                       Remove a table schema, and all of its contained data");
			System.out.println();
			System.out.println("2.DML Commands:");
			System.out.println(
					"\t(a)INSERT INTO table_name [column_list] VALUES value_list;        Inserts a single record into a table");
			System.out.println(
					"\t(b)DELETE FROM TABLE table_name WHERE [condition];                Removes a particular record based on condition");
			System.out.println(
					"\t(c)UPDATE table_name SET column_name = value WHERE [condition];   Modifies one or more records in a table");
			System.out.println();
			System.out.println("3.VDL Commands: ");
			System.out.println(
					"\t(a)SELECT * FROM <table_name>;                                    Display all records in the table");
			System.out.println(
					"\t(b)SELECT * FROM <table_name> WHERE rowid = <value>;              Display records satisfying a particular condition");
			System.out.println();
			System.out.println(
					"4.CREATE DATABASE <database_name>;                                        Creates a database");
			System.out.println();
			System.out.println(
					"5.SHOW DATABASES;                                                         Shows the list of all databases");
			System.out.println();
			System.out.println(
					"6.DROP DATABASE <database_name>;                                          Deletes a database");
			System.out.println();
			System.out.println(
					"7.EXIT;                                                                   Exit the program");
			System.out.println();
			System.out.println(
					"8.HELP;                                                                   Show this help information");
			System.out.println();
			break;

		case "exit":
			System.out.println();
			break;

		case "version":
			System.out.println();
			System.out.println("DavisBase Version 1.0");
			System.out.println();
			break;

		default:
			System.out.println();
			System.out.println("Incorrect input. Please check the help section to know the supported commands");
			System.out.println();
			break;

		}
	}

}