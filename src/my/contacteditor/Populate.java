package my.contacteditor;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import static java.lang.Math.toIntExact;
        
class DbConnection {

    Connection conn = null;
     
    public Connection getConnection()
    {
        
        //JDBC Driver name and url
        String JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";  
        String DB_URL = "jdbc:mysql://localhost/";
        
        //Database Credentials
        String user = "ADI";
        String password = "Aryan@3112";
        String port = "1521";
        String DBname = "SYSTEM";
        
        try {
            
            //Register JDBC Driver
            Class.forName("oracle.jdbc.driver.OracleDriver"); 
            
            //Open a connection
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:xe", user, password);
            System.out.println("Database created successfully...");
            return conn;   
        }
        catch(ClassNotFoundException e){
            System.out.println("Connection failed!" + e);
            return null;
        }
        catch(SQLException se){
            System.out.println("Connection failed!" + se);
            se.printStackTrace();
        }
        catch(Exception e){
            //Handle errors for Class.forName
            e.printStackTrace();
        } 
        return null;
    }
}

public class Populate {
    public static void main(String args[]) throws IOException, SQLException, java.text.ParseException{
        
        String[] file = new String[5];
        int cnt = 0;
        for(int i=0;i <args.length;i++){
            file[i] = args[i];
            System.out.println(file[i]);
            cnt ++;
        }
        
        DbConnection A1 = new DbConnection();
        Connection con = A1.getConnection(); 
        
        try{ 
            
            JSONParser jsonParser = new JSONParser();
            
            
            //Deleting previous records to avoid redundancy
       
            String query5 = "delete from Review_Info";
            PreparedStatement prepared_statement8 = con.prepareStatement(query5);
            prepared_statement8.executeUpdate();
            prepared_statement8.close();
            
            String query4 = "delete from Checkin_Info";
            PreparedStatement prepared_statement6 = con.prepareStatement(query4);
            prepared_statement6.executeUpdate();
            prepared_statement6.close();
            
            String query3 = "delete from Business_Category";
            PreparedStatement prepared_statement5 = con.prepareStatement(query3);
            prepared_statement5.executeUpdate();
            prepared_statement5.close();
            
            String query2 = "delete from Business_Info";
            PreparedStatement prepared_statement2 = con.prepareStatement(query2);
            prepared_statement2.executeUpdate();
            prepared_statement2.close();
            
            String query1 = "delete from Yelp_user";
            PreparedStatement prepared_statement1 = con.prepareStatement(query1);
            prepared_statement1.executeUpdate();
            prepared_statement1.close();
     
             
            Object obj1 = jsonParser.parse(new FileReader("D:\\ms\\2\\database\\assignment\\YelpDataset\\YelpDataset-CptS451\\yelp_user.json"));
            JSONArray jsonArray1;
            jsonArray1 = (JSONArray)obj1;
            
            Object obj2 = jsonParser.parse(new FileReader("D:\\ms\\2\\database\\assignment\\YelpDataset\\YelpDataset-CptS451\\yelp_business.json"));
            JSONArray jsonArray2;
            jsonArray2 = (JSONArray)obj2;
            
            Object obj3 = jsonParser.parse(new FileReader("D:\\ms\\2\\database\\assignment\\YelpDataset\\YelpDataset-CptS451\\yelp_checkin.json"));
            JSONArray jsonArray3;
            jsonArray3 = (JSONArray)obj3;
            
            Object obj4 = jsonParser.parse(new FileReader("D:\\ms\\2\\database\\assignment\\YelpDataset\\YelpDataset-CptS451\\yelp_review.json"));
            JSONArray jsonArray4;
            jsonArray4 = (JSONArray)obj4;
            
            // yelp_user
            String yelping_since,name,user_id,type;
            Long useful,funny,cool,fans,review_count;
            Double average_stars;
            int count1=0;
           
         
            //Inserting into Yelp_User
            for(int i=0;i<jsonArray1.size();i++)
            {
                JSONObject jsonObject1 = (JSONObject) jsonArray1.get(i);
                name = (String) jsonObject1.get("name");
                user_id = (String) jsonObject1.get("user_id");
                yelping_since = (String) jsonObject1.get("yelping_since") + "-01"; 
                JSONArray friends = (JSONArray) jsonObject1.get("friends");
                int friends_number = friends.size();
                review_count = (Long) jsonObject1.get("review_count");
                fans = (Long) jsonObject1.get("fans");
                average_stars = (Double) jsonObject1.get("average_stars");
                type = (String) jsonObject1.get("type");
                
                SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd" );  // United States style of format.
                java.util.Date myDate = format.parse(yelping_since);  // Notice the ".util." of package name.
                
                PreparedStatement prepared_statement = con.prepareStatement("INSERT INTO Yelp_User(YelpId,Name,yelping_since,Friends_number,Review_count,fans,average_stars,type) VALUES(?,?,?,?,?,?,?,?)");
                prepared_statement.setString(1, user_id);
                prepared_statement.setString(2, name);
                prepared_statement.setDate(3, new java.sql.Date(myDate.getTime()));
                prepared_statement.setInt(4, friends_number);
                prepared_statement.setLong(5, review_count);
                prepared_statement.setLong(6, fans);
                prepared_statement.setDouble(7, average_stars);
                prepared_statement.setString(8, type);
                prepared_statement.executeUpdate();
                prepared_statement.close();
               
            }
            
            
            
            //business
            String business_id,city,state;
            Long review_count_b;
            
            //Inserting into Business_Info
            for(int i=0;i<jsonArray2.size();i++)
            {
                JSONObject jsonObject2 = (JSONObject) jsonArray2.get(i);
                business_id = (String) jsonObject2.get("business_id");
                city = (String) jsonObject2.get("city");
                review_count_b = (Long) jsonObject2.get("review_count"); 
                state = (String) jsonObject2.get("state");
                
                PreparedStatement prepared_statement3 = con.prepareStatement("INSERT INTO Business_Info(Business_Id,city,Review_count,state) VALUES(?,?,?,?)");
                prepared_statement3.setString(1, business_id);
                prepared_statement3.setString(2, city);
                prepared_statement3.setLong(3, review_count_b);
                prepared_statement3.setString(4, state);
                prepared_statement3.executeUpdate();
                prepared_statement3.close();
                
            }
           
            //Business_category
            
            String[] categories = new String[28];
            categories[0] = "Active Life";
            categories[1] = "Arts & Entertainment";
            categories[2] = "Automotive";
            categories[3] = "Car Rental";
            categories[4] = "Cafes";
            categories[5] = "Beauty & Spas";
            categories[6] = "Convenience Stores";
            categories[7] = "Dentists"; 
            categories[8] = "Doctors";
            categories[9] = "Drugstores";
            categories[10] = "Department Stores";
            categories[11] = "Education";
            categories[12] = "Event Planning & Services";
            categories[13] = "Flowers & Gifts";
            categories[14] = "Food";
            categories[15] = "Health & Medical";
            categories[16] = "Home Services";
            categories[17] = "Home & Garden";
            categories[18] = "Hospitals";
            categories[19] = "Hotels & Travel";
            categories[20] = "Hardware Stores";
            categories[21] = "Grocery";
            categories[22] = "Medical Centers";
            categories[23] = "Nurseries & Gardening";
            categories[24] = "Nightlife";
            categories[25] = "Restaurants"; 
            categories[26] = "Shopping"; 
            categories[27] = "Transportation"; 
            
            JSONArray category;
            String[] individual_category = new String[100];
            int count = 0, flag=0, m=0, n=0;
            String[] business_category = new String[50];
            String[] subcategory = new String[50];
            
            //Inserting into Business_Category
            for(int i=0;i<jsonArray2.size();i++){
                JSONObject jsonObject3 = (JSONObject) jsonArray2.get(i);
                business_id = (String) jsonObject3.get("business_id");
                category = (JSONArray)jsonObject3.get("categories");
                for(int j=0;j<category.size();j++){
                    individual_category[j] = (String)category.get(j);
                    count = count + 1;
                }
                for(int k=0; k<count; k++){
                    for(int l=0;l<28;l++){
                        if(individual_category[k].equals(categories[l]))
                        {
                            flag = 1;
                            break;
                        }
                    }
                    if(flag == 1){
                        business_category[m] = individual_category[k];
                        m = m + 1;
                        flag = 0;
                    }
                    else{
                        subcategory[n] = individual_category[k];
                        n = n + 1;
                    }
                }
                for(int p=0;p<m;p++){
                    for(int q=0; q<n; q++){
                        PreparedStatement prepared_statement4 = con.prepareStatement("INSERT INTO Business_Category(business_id,business_category,subcategory) VALUES(?,?,?)");
                        prepared_statement4.setString(1, business_id);
                        prepared_statement4.setString(2, business_category[p]);
                        prepared_statement4.setString(3, subcategory[q]);
                        prepared_statement4.executeUpdate();
                        prepared_statement4.close();
                    }
                } 
                count = 0;
                m = 0;
                n = 0;
            }
            
                    
            
            
            //Checkin_Info
            JSONObject checkin_info;
            Long no_of_checkin;
            Set<String> keys;
            String[] timing = new String[10];
            int iterate=0,time,hour;
            String day=null;
            
            //Inserting into checkin_info
            for(int i=0;i<jsonArray3.size();i++){
                JSONObject jsonObject4 = (JSONObject) jsonArray3.get(i);
                checkin_info = (JSONObject) jsonObject4.get("checkin_info");
                business_id = (String)jsonObject4.get("business_id");
                keys = checkin_info.keySet();
                
                for(String key:keys){
                    no_of_checkin = (Long) checkin_info.get(key);
                    for (String x: key.split("-")) {
                        timing[iterate] = x;
                        iterate = iterate+1;
                    }
                    iterate=0;
                    hour = Integer.parseInt(timing[0]);
                    time = Integer.parseInt(timing[1]);
                    
                    PreparedStatement prepared_statement7 = con.prepareStatement("INSERT INTO Checkin_Info(business_id,hour,day,no_of_checkin) VALUES(?,?,?,?)");
                    prepared_statement7.setString(1, business_id);
                    prepared_statement7.setInt(2, hour);
                    prepared_statement7.setInt(3, time);
                    prepared_statement7.setLong(4, no_of_checkin);
                    prepared_statement7.executeUpdate();
                    prepared_statement7.close();
                    
                } 
            }
                 
                
            //Review_Info
                    
            String review_id,review_text,r_date=null,review_type;
            Long stars, votes = 0L;
            int review_votes;
            JSONObject votes_info;
            Set<String> review_keys;
            SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd" );  // United States style of format. 
            
             //Inserting into Review_Info
            for(int i=0;i<jsonArray4.size();i++)
            {
                JSONObject jsonObject5 = (JSONObject) jsonArray4.get(i);
                
                votes_info = (JSONObject) jsonObject5.get("votes");
                review_keys = votes_info.keySet();
                for(String r_key:review_keys){
                    votes = votes + (Long)votes_info.get(r_key);   
                }
                review_votes = toIntExact(votes);
                review_id = (String) jsonObject5.get("review_id");
                user_id = (String) jsonObject5.get("user_id");
                business_id = (String) jsonObject5.get("business_id"); 
                stars = (Long) jsonObject5.get("stars");
                review_text = (String) jsonObject5.get("text");
                review_type = (String) jsonObject5.get("type");
                r_date = (String) jsonObject5.get("date");
                java.util.Date review_date = format.parse(r_date);
                
                PreparedStatement prepared_statement9 = con.prepareStatement("INSERT INTO Review_Info(review_id,YelpId,business_id,votes,stars,type,review_date,review_text) VALUES(?,?,?,?,?,?,?,?)");
                prepared_statement9.setString(1, review_id);
                prepared_statement9.setString(2, user_id);
                prepared_statement9.setString(3, business_id);
                prepared_statement9.setInt(4, review_votes);
                prepared_statement9.setLong(5, stars);
                prepared_statement9.setString(6, review_type);
                prepared_statement9.setDate(7, new java.sql.Date(review_date.getTime()));
                prepared_statement9.setString(8, review_text);
                prepared_statement9.executeUpdate();
                prepared_statement9.close();
            }
            
        }
       catch (ParseException ex) {
            Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, ex);
        }       
        finally{
            try {
                if(con!=null){
                    con.close();
                }
            } 
            catch(SQLException e){
                        e.printStackTrace();
            }
        }
    }
}
