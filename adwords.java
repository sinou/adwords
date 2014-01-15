import java.io.*;
import java.sql.*;
 
public class adwords{
	
	
	public static void main(String[] argv) throws Exception {
		String[] sArray = new String[8];
		long time = System.currentTimeMillis();
		try{
		FileInputStream fstream = new FileInputStream("system.in");
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		int i = 0;
		while ((sArray[i] = br.readLine()) != null)   {
			sArray[i] = sArray[i].split("=")[1].trim();
			System.out.println (sArray[i]);
			i++;
		}
		in.close();
		}catch(Exception e){
			
		}
		System.out.println("-------- Oracle JDBC Connection Testing ------");
 
		try {
 
			Class.forName("oracle.jdbc.driver.OracleDriver");
 
		} catch (ClassNotFoundException e) {
 
			System.out.println("Where is your Oracle JDBC Driver?");
			e.printStackTrace();
			return;
 
		}
 
		System.out.println("Oracle JDBC Driver Registered!");
 
		Connection connection = null;
 
		try {
 
			connection = DriverManager.getConnection(
					"jdbc:oracle:thin:@//oracle1.cise.ufl.edu:1521/orcl", sArray[0],
					sArray[1]);
 
		} catch (SQLException e) {
 
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
 
		}
		ResultSet rset = null;
		if (connection != null) {
			System.out.println(" -|||- ");
			
			Process p = Runtime.getRuntime().exec("sqlplus " + sArray[0] + "@orcl/" + sArray[1] + " @adwords.sql");
	        p.waitFor();
	        
	         //CallableStatement cstmt;
	         System.out.println(" ||| ");

		  System.out.println("Time taken in Milliseconds (establishing connection): " + (System.currentTimeMillis() - time) );
            	  time = System.currentTimeMillis();
	        
	         Process proc3 = Runtime.getRuntime().exec("sqlldr " + sArray[0] + "/" + sArray[1] + "@orcl DATA=Keywords.dat CONTROL=Keywords.ctl LOG=Keywords.log");
	         proc3.waitFor();	         
	         
	         Process proc1 = Runtime.getRuntime().exec("sqlldr " + sArray[0] + "/" + sArray[1] + "@orcl DATA=Advertisers.dat CONTROL=Advertisers.ctl LOG=Advertiser.log"); 
	         proc1.waitFor();
	         	         
	         
	         
	         Process proc2 = Runtime.getRuntime().exec("sqlldr " + sArray[0] + "/" + sArray[1] + "@orcl DATA=Queries.dat CONTROL=Queries.ctl LOG=Queries.log");
	         proc2.waitFor();
	         	         System.out.println(" ||| ");


                System.out.println("Time taken in Milliseconds (loading dat): " + (System.currentTimeMillis() - time) );
            	  time = System.currentTimeMillis();

		  
            
	         CallableStatement callableStatement = null;
				String storeProc = "{call sq(?,?,?,?,?,?)}";
				
				try 
				{
					callableStatement = connection.prepareCall(storeProc);
					callableStatement.setInt(1, Integer.parseInt(sArray[2]));
					callableStatement.setInt(2, Integer.parseInt(sArray[3]));
					callableStatement.setInt(3, Integer.parseInt(sArray[4]));
					callableStatement.setInt(4, Integer.parseInt(sArray[5]));
					callableStatement.setInt(5, Integer.parseInt(sArray[6]));
					callableStatement.setInt(6, Integer.parseInt(sArray[7]));
					callableStatement.executeUpdate();
				} 
				
				catch (SQLException e) 
				{
					System.out.println(e.getMessage());
				}
		   		
				/*callableStatement = connection.prepareCall("{call sq( " + 
            Integer.parseInt(sArray[2]) + ", " + 
            Integer.parseInt(sArray[3]) + ", " + 
            Integer.parseInt(sArray[4]) + ", " + 
            Integer.parseInt(sArray[5]) + ", " + 
            Integer.parseInt(sArray[6]) + ", " + 
            Integer.parseInt(sArray[7]) + " ) }");*/
            System.out.println(" ||| ");
             
	     System.out.println("Time taken in Milliseconds (sql procedure): " + (System.currentTimeMillis() - time) );
            time = System.currentTimeMillis();
			
            FileWriter fw = null;
            File file = null;
            PrintWriter pw = null;
            Statement resStmt = null;
            ResultSet resRs = null;
            int qid = 0;
            int rank;
            
            for(int i = 0; i < 6; i ++){
            	
            	file = new File("system.out." + (i + 1));
            	try
    			{
    				if(!file.exists())
    					file.createNewFile();
    				else
    					file.delete();
    				
    				fw = new FileWriter(file.getPath(),true);
    			}
    			
    			catch(Exception e)
    			{
    				 System.out.println(e.getMessage());
    			}
            	pw = new PrintWriter(fw);
            	resStmt = connection.createStatement();
            	resRs = resStmt.executeQuery("SELECT * FROM OUTPUT" + (i + 1) + " order by qid asc, adrank asc");
            	while (resRs.next()) 
    	   		{
    	            qid = resRs.getInt("QID");
    	            rank = (int) resRs.getFloat("ADRANK");
    	            int advertiserId = resRs.getInt("ADVERTISERID");
    	            float balance = resRs.getFloat("BALANCE");
    	            float budget = resRs.getFloat("BUDGET");
    	            StringBuffer resStr = new StringBuffer(); 
    	            resStr.append(qid + "," + rank + "," + advertiserId + "," + balance + "," + budget);
    	            // System.out.println(resStr.toString());
    	            pw.println(resStr.toString());    
    	   		}
                
    	   		pw.close();
            }
            
            
		} else {
			System.out.println("Failed to make connection!");
		}
		
                System.out.println("Time taken in Milliseconds (file write): " + (System.currentTimeMillis() - time) );
	}
 
}


