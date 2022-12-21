import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.Map.Entry;
import oracle.jdbc.driver.*;
import org.apache.ibatis.jdbc.ScriptRunner;
// truncate in final output

public class DBRetriever{
    static Connection con;
    static Statement stmt;
    
    
    public static String removeLastChar(String s, int num) {
        return (s == null || s.length() == 0)
          ? null 
          : (s.substring(0, s.length() - num));
    }
    
    public static void viewTable(Scanner sc)
    {
    	System.out.print("MOVIES (Yes/No):");
    	String moviesAns=null;
    	String ratingAns=null;
    	moviesAns=sc.nextLine();
    	while (!moviesAns.equalsIgnoreCase("yes") && !moviesAns.equalsIgnoreCase("no"))
		{
			System.out.println("Error: Please enter a valid option. [ Yes/ No ]");
			moviesAns=sc.nextLine();
		}
    	
    	if(moviesAns.equalsIgnoreCase("yes"))
    	{
	    
    		// output the sql query
    		  String query = "select * from Movies";
    		    try (Statement stmt = con.createStatement()) {
    		      ResultSet rs = stmt.executeQuery(query);
    		      if(rs.isBeforeFirst())
    		      {
    		    	System.out.println("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
      		        System.out.printf("%-10s %-40s %-10s %-45s %-45s %-30s %-10s", "movieid", "title", "language", "production_company", "production_country","release_date", "runtime");
      		        System.out.println();
    		    	System.out.println("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
    		      }
    		      else {
    	   		    	 System.out.println("Table is Empty");
    	  		      }
    		      
    		      while (rs.next()) {
    		        String movieid = rs.getString("movieid");
    		        String title = rs.getString("title");
    		        if(title!=null)
    		        {
    		        	title=title.substring(0,Math.min(title.length(), 30));
    		        	if(title.length()==30)
    		        	title+="........";
    		        }
    		        String language = rs.getString("language");
    		        String production_company = rs.getString("production_company");
    		        if(production_company!=null)
    		        {
    		        	production_company=production_company.substring(0,Math.min(production_company.length(), 30));
    		        	if(production_company.length()==30)
    		        	production_company+="........";
    		        }
    		        String production_country = rs.getString("production_country");
    		        if(production_country!=null)
    		        {
    		        	production_country=production_country.substring(0,Math.min(production_country.length(), 30));
    		        	if(production_country.length()==30)
    		        	production_country+="........";
    		        }
    		        String release_date = rs.getString("release_date");     
    		        String runtime = rs.getString("runtime");
    		   
    		        System.out.format("%-10s %-40s %-10s %-45s %-45s %-30s %-10s",movieid, title, language, production_company, production_country,release_date, runtime);
    		            System.out.println();	        
    		      }
  		    	System.out.println("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
    		    } catch (SQLException e) {
    		      System.out.println("SQL query doesnt fetch any results");
    		    }
    	}
    	
        System.out.println("");
    	System.out.print("RATINGS (Yes/No):");
    	ratingAns= sc.nextLine();

    	while (!ratingAns.equalsIgnoreCase("yes") && !ratingAns.equalsIgnoreCase("no"))
		{
			System.out.println("Error: Please enter a valid option. [ Yes/ No ]");
			ratingAns=sc.nextLine();
		}
    	
    	if(ratingAns.equalsIgnoreCase("yes"))
    	{
	 
    		// output the sql query\
    		  String query = "select * from Ratings";
  		    try (Statement stmt = con.createStatement()) {
  		      ResultSet rs = stmt.executeQuery(query);
  		      if(rs.isBeforeFirst())
  		      {
  		    	System.out.println("-------------------------------------------------------");
  		        System.out.printf("%-10s %-20s %-20s", "movieid", "userid", "rating");
  		        System.out.println();
  		        System.out.println("-------------------------------------------------------");
  		      }
  		      else {
   		    	 System.out.println("Table is Empty");

  		      }
  		      while (rs.next()) {
  		        String movieid = rs.getString("movieid");
  		        String userid = rs.getString("userid");
  		        float rating = rs.getFloat("rating");
  		      System.out.format("%-10s %-20s %-20.1f ", movieid, userid, rating);
	            System.out.println();
  		      }
		      System.out.println("---------------------------------------------------------");

  		    } catch (SQLException e) {
  		      System.out.println("SQL query doesnt fetch any results");
  		    }
    	}
 
    	
    }
    
    
    public static void searchByMovieId(Scanner sc)
    {
    	int id= sc.nextInt();
    	 // String query = "select *, AVG(rating) as averageRating from Movies as M Inner Join Ratings as R on movieid = " + id + "group by movieid";
    	  String query= " select m.movieid, m.title,m.language,m.production_company,m.production_country,m.runtime, m.release_date, avg(r.rating) as average_rating from movies m\n"
    	  		+ "inner join ratings r on m.movieid = r.movieid \n"
    	  		+ "group by m.movieid, m.title,m.language,m.production_company,m.production_country,m.runtime, m.release_date \n"
    	  		+ "having m.movieid = " + id;
    	  
		    try (Statement stmt = con.createStatement()) {
		      ResultSet rs = stmt.executeQuery(query);
		  
		      if(rs.isBeforeFirst())
		      {
  		    	System.out.println("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
      		        System.out.printf("%-10s %-40s %-10s %-40s %-40s %-30s %-12s %-10s", "movieid", "title", "language", "production_company", "production_country","release_date", "runtime","average_rating");
      		        System.out.println();
    		    	System.out.println("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
		      }
		      else {
	   		    	 System.out.println("SQL query doesnt fetch any results");
	  		      }
		      
		      while (rs.next()) {
		        String movieid = rs.getString("movieid");
		        String title = rs.getString("title");
		        String language = rs.getString("language");
		        String production_company = rs.getString("production_company");
		        String production_country = rs.getString("production_country");
		        String release_date = rs.getString("release_date");     
		        String runtime = rs.getString("runtime");
		        float average_rating = rs.getFloat("average_rating");
		        
		        if(title!=null)
		        {
		        	title=title.substring(0,Math.min(title.length(), 30));
		        	if(title.length()==30)
		        	title+="........";
		        }
		        if(production_company!=null)
		        {
		        	production_company=production_company.substring(0,Math.min(production_company.length(), 30));
		        	if(production_company.length()==30)
		        	production_company+="........";
		        }
		        if(production_country!=null)
		        {
		        	production_country=production_country.substring(0,Math.min(production_country.length(), 30));
		        	if(production_country.length()==30)
		        	production_country+="........";
		        }
		        
		        System.out.format("%-10s %-40s %-10s %-40s %-40s %-30s %-12s %-10.1f", movieid, title, language,production_company,production_country,release_date,runtime,average_rating);
	            System.out.println();
		      }
		    	System.out.println("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");

		    } catch (SQLException e) {
		      System.out.println("SQL query doesnt fetch any results");
		    }
		    
	        System.out.println("");

		
    	
    }  
    public static void searchByAttribute(Scanner sc)
    {
    	LinkedHashMap<String, String> inputHp= new LinkedHashMap<>();
    	LinkedHashMap<String, String> outputHp= new LinkedHashMap<>();

    	
    String inputArray[]= new String[] {"movieid","title","production_company","userid","rating"};
    String outputArray[]= new String[] {"movieid","title","language","production_company","production_country","release_date", "runtime","userid","rating"};
    String inputAns=null;
    String outputAns=null;
    int outputNullCount;
    int inputNullCount;
    int outputFlag=1;
    int inputFlag=1;

while(inputFlag==1)
{
	inputNullCount=0;
	  for(int i=0;i<inputArray.length;i++)
	    {
	    	System.out.print(inputArray[i]+": ");
	    	inputAns= sc.nextLine();
	    	inputHp.put(inputArray[i],inputAns);
	    	if(inputAns == null || inputAns.length() == 0)
	    	{
	    		inputNullCount++;
	    	}
	    	
	    }
	  
	  if(inputNullCount==inputArray.length)
	    {
	    	System.out.println("You must select atleast one input Attributes");
	    	
	    }
	    else {
	    	inputFlag=0;
	    }
}

  
while(outputFlag==1)
{
	outputNullCount=0;	
    for(int i=0;i<outputArray.length;i++)
    {
    	System.out.print(outputArray[i] + "( Yes/No ) : ");
    	outputAns= sc.nextLine();
    	while (!outputAns.equalsIgnoreCase("yes") && !outputAns.equalsIgnoreCase("no"))
		{
			System.out.println("Error: Please enter a valid option. [ Yes/ No ]");
			outputAns=sc.nextLine();
			if(outputAns.equalsIgnoreCase("no"))
				outputNullCount++;
			
		}
    	outputHp.put(outputArray[i], outputAns);
    	
    }
    if(outputNullCount==outputArray.length)
    {
    	System.out.println("You must select atleast one output Attributes");
    	
    }
    else {
    	outputFlag=0;
    }
    
}

String outputQuery="";
String inputQuery="";
String outputheading[]= new String[10];
int k=0;
for(Map.Entry<String, String> set: outputHp.entrySet())
{
	if(set.getValue().equalsIgnoreCase("yes"))
	{
		if(set.getKey().equalsIgnoreCase("userid") || set.getKey().equalsIgnoreCase("rating"))
		{
		outputQuery+="R."+set.getKey()+",";
		}
		else {
			outputQuery+="M."+set.getKey()+",";
		}
		outputheading[k]= set.getKey();
		k++;
	}
}
int outputLength=k;
String queryOutput[]= new String[10];
outputQuery=removeLastChar(outputQuery,1);
//System.out.println(outputQuery);

for(Map.Entry<String, String> inset: inputHp.entrySet())
{
	if(!(inset.getValue().length()==0 || inset.getValue()==null))
	{
		if(inset.getKey().equalsIgnoreCase("userid") || inset.getKey().equalsIgnoreCase("rating"))
		{
			inputQuery+="R."+inset.getKey()+"="+ "\'"+inset.getValue()+ "\'" + " AND ";	
		}
		else if(inset.getKey().equalsIgnoreCase("title"))
		{
			inputQuery+="M."+inset.getKey()+" LIKE "+ "\'%" + inset.getValue()+ "%\'" + " AND ";	
		}
		else {
			inputQuery+="M."+inset.getKey()+"="+ "\'" + inset.getValue()+ "\'" + " AND ";	
		}
	}
}
inputQuery=removeLastChar(inputQuery,5);
//System.out.println(inputQuery);


String query = "select " + outputQuery + " from Movies M left join Ratings r on m.movieid = r.movieid where " + inputQuery;

//System.out.println(query);


try (Statement stmt = con.createStatement()) {
  ResultSet rs = stmt.executeQuery(query);

  if(rs.isBeforeFirst())
  {
  System.out.println("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
	  for(int i=0;i<outputLength;i++) {
		  if(outputheading[i].equalsIgnoreCase("movieid") || outputheading[i].equalsIgnoreCase("language") || outputheading[i].equalsIgnoreCase("runtime") || outputheading[i].equalsIgnoreCase("rating") || outputheading[i].equalsIgnoreCase("userid"))
	        System.out.printf("%-10s ", outputheading[i]);
		  else if(outputheading[i].equalsIgnoreCase("release_date"))
	    	  System.out.printf("%-30s ", outputheading[i]);
		  else
			    System.out.printf("%-40s ", outputheading[i]);
	  }
	  System.out.println("");
  	System.out.println("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
  
  }
  else {
	    	 System.out.println("Table is Empty");
	      }
  while (rs.next()) {
    for(int i=0;i<outputLength;i++) {
    	queryOutput[i] = rs.getString(outputheading[i]); 
    	 
        if(outputheading[i].equalsIgnoreCase("title") ||outputheading[i].equalsIgnoreCase("production_company") || outputheading[i].equalsIgnoreCase("production_country") )
        {
        	if(queryOutput[i]!=null)
        	{
        	queryOutput[i]=queryOutput[i].substring(0,Math.min(queryOutput[i].length(), 30));
        	if(queryOutput[i].length()==30)
        		queryOutput[i]+="........";
        	}
        }
        
		  if(outputheading[i].equalsIgnoreCase("movieid") || outputheading[i].equalsIgnoreCase("language") || outputheading[i].equalsIgnoreCase("runtime") || outputheading[i].equalsIgnoreCase("rating") || outputheading[i].equalsIgnoreCase("userid"))
    	  System.out.printf("%-10s ", queryOutput[i]);
		  else if(outputheading[i].equalsIgnoreCase("release_date"))
	    	  System.out.printf("%-30s ", queryOutput[i]);
		  else
	    	  System.out.printf("%-40s ", queryOutput[i]);

    	// System.out.print(queryOutput[i]+", ");
  	  }
    System.out.println("");
  }
	System.out.println("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
	  System.out.println("");

 
} catch (SQLException e) {
  System.out.println("SQL query doesnt fetch any results");
}
    }  
   

    
    public static void main(String argv[])
    {
	connectToDatabase();
    }

    public static void connectToDatabase()
    {
	String driverPrefixURL="jdbc:oracle:thin:@";
	String jdbc_url="artemis.vsnet.gmu.edu:1521/vse18c.vsnet.gmu.edu";
	int flag=1;
    Scanner sc = new Scanner(System.in);
	   while(flag==1)
       {

		   // Create a Scanner object
			System.out.println("Enter username");

			String userName = sc.nextLine(); // Read user input

			System.out.println("Enter Password");

			String myPassword = sc.nextLine(); // Read user input
    
	// IMPORTANT: DO NOT PUT YOUR LOGIN INFORMATION HERE. INSTEAD, PROMPT USER FOR HIS/HER LOGIN/PASSWD
        String username=userName;
        String password=myPassword;
	
        try{
	    //Register Oracle driver
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
        } catch (Exception e) {
            System.out.println("Failed to load JDBC/ODBC driver.");
            return;
        }

     
       try{
            System.out.println(driverPrefixURL+jdbc_url);
            con=DriverManager.getConnection(driverPrefixURL+jdbc_url, username, password);
            DatabaseMetaData dbmd=con.getMetaData();
            stmt=con.createStatement();

            System.out.println("Connected.");

            if(dbmd==null){
                System.out.println("No database meta data");
            }
            else {
                System.out.println("Database Product Name: "+dbmd.getDatabaseProductName());
                System.out.println("Database Product Version: "+dbmd.getDatabaseProductVersion());
                System.out.println("Database Driver Name: "+dbmd.getDriverName());
                System.out.println("Database Driver Version: "+dbmd.getDriverVersion());
                flag=0;
            }
        }catch( Exception e) {
            System.out.println(e.getMessage());
        	}
        }
	  
       ScriptRunner sr = new ScriptRunner(con);
       //Creating a reader object
       Reader reader = null;
   
       while(true)
       {
    try {
 	   // enter location of script file.
        
    	System.out.println("Enter location of Script File");
        String location = sc.nextLine();
		reader = new BufferedReader(new FileReader(location));
		if(reader.toString()!=null)
			break;
			
	} catch (FileNotFoundException e) {
        System.out.println(e.getLocalizedMessage());
	}
       }
       try {
    	   // skip the set define off as it is giving error on Java code
		reader.skip(15);
	} catch (IOException e) {
		// TODO Auto-generated catch block
		System.out.println("SET DEFINE OFF; STATEMENT IS MISSING");
	}
       sr.runScript(reader);
       int flag3=1;
       while(flag3==1)
       {
    		System.out.println("MENU");
    		System.out.println("(1) View table contents");
    		System.out.println("(2) Search by MOVIEID");
    		System.out.println("(3) Search by one or more attributes");
    		System.out.println("(4) Exit");
    		int userchoice= sc.nextInt();
    		sc.nextLine();
    		switch(userchoice) {
    		case 1: 
        		System.out.println("Enter the names of tables you want to view ");
        		viewTable(sc);
        		break;
    		case 2: 
        		System.out.println("Enter the MovieID from MOVIES Table ");
        		searchByMovieId(sc);
        		break;

    		case 3: 
        		System.out.println("Enter one or more input attributes from [MovieID, Title, Production_company, ReviewerID, Rating] and output attributes");
        		searchByAttribute(sc);
        		break;

    		case 4: 
        		System.out.println("GoodBye! ");
        		System.exit(0);
        		break;
        	default:
        		System.out.println("ERROR: Please enter number between [1-4] inclusive");
        		break;
    		}
    			
       }
	   
	   

    }// End of connectToDatabase()
}// End of class
