package transactionManagement;

import java.io.FileInputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Scanner;

public class transactionManager {

	public static void main(String args[]) throws Exception {
		
		System.out.println("Starting Program\n");
		
		String url = "jdbc:mysql://localhost:3306/myDatabase";
		String uname = "root";
		String pass = "root";
		Class.forName("com.mysql.cj.jdbc.Driver");

		// Establish connection to database
		Connection con = DriverManager.getConnection(url, uname, pass);
		
		Statement st = con.createStatement();
		ResultSet rs = null;
		String sql = "";
		

		// Drop tables if they already exist
		sql = "DROP TABLE IF EXISTS `person`";
		st.executeUpdate(sql);

		sql = "DROP TABLE IF EXISTS `likes`";
		st.executeUpdate(sql);
		

		// Create person table
		sql = "CREATE TABLE `myDatabase`.`person` (\n" +
				"  `pid` INT NOT NULL,\n" + 
				"  `name` VARCHAR(45) NULL,\n" + 
				"  `age` INT NULL,\n" + 
				"  PRIMARY KEY (`pid`));";
		st.executeUpdate(sql);
		
		System.out.println("Successfully created 'person' table");
		
		
		// Create likes table
		sql = "CREATE TABLE `myDatabase`.`likes` (\n" +
				"  `pid` INT NOT NULL,\n" + 
				"  `mid` VARCHAR(45) NOT NULL,\n" + 
				"  PRIMARY KEY (`pid`, `mid`));";
		st.executeUpdate(sql);
		
		System.out.println("Successfully created 'likes' table\n");


		// READ TRANSFILE
		
		System.out.println("Opening transfile...");
		FileInputStream fis = new FileInputStream("transfile.txt");       
		Scanner sc=new Scanner(fis);    //file to be scanned 
		
		int transNumber = 1;
		while(sc.hasNextLine()) {

			String transaction = sc.nextLine();
			
			System.out.println("\nTransaction " + transNumber + ": (" + transaction + ")");
			transNumber++;
			
			String[] words = transaction.split(" ");


			// TRANSACTION 1
			// Delete An Existing Person From The Database
			if (words[0].compareTo("1") == 0) {
				sql = "SELECT pid FROM person WHERE pid = " + words[1] + ";";
				rs = st.executeQuery(sql);
				
				if ( rs.next()) {
					sql = "DELETE FROM person " +
			                   "WHERE pid = " + words[1] + ";";
					st.executeUpdate(sql);
					
					sql = "DELETE FROM likes " +
			                   "WHERE pid = " + words[1] + ";";
					st.executeUpdate(sql);
					
					sql = "DELETE FROM likes " +
			                   "WHERE mid = " + words[1] + ";";
					st.executeUpdate(sql);
					
					System.out.println("done");
				}
				
				else System.out.println("error");
			}


			// TRANSACTION 2
			// Insert A New Person Into The Database
			else if (words[0].compareTo("2") == 0) {
				boolean isMidPresentInPerson = true;
				boolean isDuplicate = false;
				
				sql = "SELECT pid FROM person WHERE pid = " + words[1] + ";";
				rs = st.executeQuery(sql);

				if (rs.next())
					isDuplicate = true;

				if ( isDuplicate)
					System.out.println("error: pid " + words[1] + " is already in the persons table");
				else {
					sql = "INSERT INTO `person` " +
			                "VALUES (" + words[1] + ", '" + words[2] + "', " + words[3] +")";
					st.executeUpdate(sql);
					
					for (int i = 4; i < words.length; ++i) {
						sql = "SELECT pid FROM person WHERE pid = " + words[i] + ";";
						rs = st.executeQuery(sql);
						
						if (rs.next() == false) {
							isMidPresentInPerson = false;
							System.out.println( "error: (" + words[1] + ", " + words[i] 
									+ ") cannot be added to the likes relation because "
									+ words[i] + " is not in the persons relation.");
						}
						else {
							sql = "INSERT INTO `likes` " +
					                "VALUES (" + words[1] + ", " + words[i] + ")";
							st.executeUpdate(sql);
						}
					}
				}
				
				if ( !isDuplicate && isMidPresentInPerson)
					System.out.println("done");
			}


			// TRANSACTION 3
			// Output The Average Age Of All Persons
			else if (words[0].compareTo("3") == 0) {
				sql = "SELECT AVG(age) FROM person;";
				rs = st.executeQuery(sql);
				
				rs.next();
				System.out.println(Math.round(rs.getFloat(1)));
			}


			// TRANSACTION 4
			// Output Names Of All Persons Liked By A Given Person
			else if (words[0].compareTo("4") == 0) {
				sql = "SELECT pid FROM person WHERE pid = " + words[1] + ";";
				rs = st.executeQuery(sql);
				if ( rs.next() == false)
					System.out.print("error: pid " + words[1] + " does not exist the person or likes relations");
				
				ArrayList<Integer> liked = new ArrayList<Integer>();
				
				findLiked( liked, con, Integer.parseInt(words[1]) ); // recursive function to efficiently gather all persons
				
				Collections.sort( liked);
				for ( int i = 0; i < liked.size(); ++i) {
					sql = "SELECT name FROM person WHERE pid = " + liked.get(i) + ";";
					rs = st.executeQuery(sql);
					rs.next();
					System.out.print( rs.getString(1) + " ");
				}
				System.out.println();
			}


			// TRANSACTION 5
			// Output The Average Age Of All Persons Liked,
			// Directly Or Indirectly, By A Given Person
			else if (words[0].compareTo("5") == 0) {
				sql = "SELECT pid FROM person WHERE pid = " + words[1] + ";";
				rs = st.executeQuery(sql);

				if ( rs.next() == false)
					System.out.println("error: pid " + words[1] + " does not exist the person or likes relations");
				else {
					ArrayList<Integer> liked = new ArrayList<Integer>();
					
					findLiked( liked, con, Integer.parseInt(words[1]) ); // gather all persons liked by given person
					
					Collections.sort( liked);

					float sum = 0;
					for ( int i = 0; i < liked.size(); ++i) {
						sql = "SELECT age FROM person WHERE pid = " + liked.get(i) + ";";
						rs = st.executeQuery(sql);
						rs.next();
						sum += rs.getInt(1);
					}
					System.out.println( Math.round( sum / liked.size()));
				}
			}


			// TRANSACTION 6
			// Output All Persons Who Directly Like Two Or More Persons
			else if (words[0].compareTo("6") == 0) {
				ArrayList<Integer> liked = new ArrayList<Integer>();
				
				sql = "SELECT pid FROM likes GROUP BY pid HAVING COUNT(*) >=2;";
				rs = st.executeQuery(sql);
				
				while( rs.next())
					liked.add( rs.getInt(1));
				
				Collections.sort( liked);
				for ( int i = 0; i < liked.size(); ++i) {
					sql = "SELECT name FROM person where pid = " + liked.get( i) + ";";
					rs = st.executeQuery( sql);
					rs.next();
					System.out.print( rs.getString(1) + " ");
				}
				
				if ( liked.size() == 0)
					System.out.print("there is no person that likes two or more persons");

				System.out.println();
			}

		}  
		sc.close();     //closes the scanner  

		
		// Output Tables

		System.out.println("\n\nOutputting 'person' table's values...");
		sql = "select * from person;";
		rs = st.executeQuery(sql);
		
		while( rs.next()){
			System.out.println(rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3));
		}
		
		System.out.println("\nOutputting 'likes' table's values");
		sql = "select * from likes;";
		rs = st.executeQuery(sql);
		
		while( rs.next())
			System.out.println(rs.getString(1) + " " + rs.getString(2));

		
		// Drop table
		sql = "DROP TABLE `person`";
		st.executeUpdate(sql);
		
		System.out.println("\n\nSuccessfully dropped 'person' table");
		
		
		// Drop table
		sql = "DROP TABLE `likes`";
		st.executeUpdate(sql);
		
		System.out.println("Successfully dropped 'likes' table");
		

		st.close();
		con.close();

	}//end program



	// findLiked:
	// 		Recursively and efficiently acquire all persons liked,
	// 		directly or indirectly, by person with primary key curPid
	//		and append them to the liked ArrayList
	private static void findLiked(ArrayList<Integer> liked, Connection con, int curPid) {

		String sql = "";
		sql = "SELECT mid FROM likes WHERE pid = " + curPid + ";";
		
		try {
			Statement st = con.createStatement();
			ResultSet rs = st.executeQuery(sql);

			while ( rs.next() == true) {
				curPid = rs.getInt(1);

				if ( liked.contains( curPid) == false) {
					liked.add( curPid);
					findLiked( liked, con, curPid);
				}
			}
		}
		catch (SQLException e) {
			System.out.println("error");
		}
	}
}
