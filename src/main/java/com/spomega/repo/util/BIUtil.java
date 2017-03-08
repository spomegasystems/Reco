package com.spomega.repo.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 
 * Utility file for BI
 */
public class BIUtil {

    protected final static String OK = "Ok";
    protected final static String FAILED = "Failed";
    protected final static int START_YEAR = 2016;
    protected final static Long DEFAULT_TIME = 1457518321908L;

    // <editor-fold defaultstate="collapsed" desc="Table Names">
    protected final static String TABLE_MOVIE  = "re_movie";
    protected final static String TABLE_BOOK  = "re_book";
    protected final static String TABLE_GAME = "re_game";
    protected final static String TABLE_GADGET = "re_gadget";
    protected final static String TABLE_PERSON = "re_person";
     protected final static String TABLE_TRANSACTION = "re_transaction";
     protected final static String TABLE_FRIENDS = "re_friend";
    
    protected final static ArrayList<String> TABLES = new ArrayList<>(Arrays.asList(TABLE_MOVIE , TABLE_BOOK , TABLE_GAME,TABLE_GADGET));
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Data Set Names">
    protected final static String DATA_SET_MOVIE = "movie_data";
    protected final static String DATA_SET_BOOK = "book_data";
    protected final static String DATA_SET_GAME = "game_data";
    protected final static String DATA_SET_GADGET = "gadget_data";
    protected final static String DATA_SET_PERSON = "person_data";
    protected final static String DATA_SET_TRANSACTION = "transaction_data";
    
    
    protected String getIndicatorTable(String data_set) throws Exception {
        switch (data_set) {
            case DATA_SET_MOVIE: return TABLE_MOVIE;
            case DATA_SET_BOOK: return TABLE_BOOK;
            case DATA_SET_GAME: return TABLE_GAME;
            case DATA_SET_GADGET: return TABLE_GADGET;
            case DATA_SET_PERSON: return TABLE_PERSON;   
            case DATA_SET_TRANSACTION: return TABLE_TRANSACTION;
            default:
                throw new Exception("Invalid data set");
        }
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Partner Names">
    protected final static String PARTNER_ACDI = "ACDIVOCA";
    protected final static String PARTNER_MOFA = "MOFA";
    protected final static String PARTNER_GF = "GRAMEEN";
    protected final static String PARTNER_CIF = "CIFCSF";
    protected final static String PARTNER_AIS_BA = "AIS_BA";
    protected final static String PARTNER_AIS_UE = "AIS_UE";
    protected final static String PARTNER_AIS_UW = "AIS_UW";
    protected final static String PARTNER_AIS_NR = "AIS_NR";
    protected final static String PARTNER_AIS_VR = "AIS_VR";
    protected final static String[] PARTNERS = {PARTNER_ACDI, PARTNER_MOFA, PARTNER_CIF, PARTNER_GF,
                                                PARTNER_AIS_BA, PARTNER_AIS_UE, PARTNER_AIS_UW,
                                                PARTNER_AIS_NR, PARTNER_AIS_VR};
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Date/time Helpers">
    protected static int getCurrentYear() {
        Calendar cal = Calendar.getInstance();
        return cal.get(Calendar.YEAR);
    }

    public List<String> getYears() {
        List<String> years = new ArrayList<>();
        Calendar cal = Calendar.getInstance();
        int year = cal.get(Calendar.YEAR);
        for(int i=year; i >= START_YEAR; i--) {
            years.add(String.valueOf(i));
        }
        return years;
    }

    protected static int getNumOfAxisMonths() {
        int year = getCurrentYear();
        int start_year = (year-1 < START_YEAR) ? START_YEAR : (year-1);
        return ((year-start_year)+1) * 12;
    }

    protected static String[] getYearMonthAxisValues() {
        int year = getCurrentYear();
        int start_year = (year-1 < START_YEAR) ? START_YEAR : (year-1);
        String[] months = {"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"};
        String[] values = new String[getNumOfAxisMonths()];

        int c=0;
        for(int y=start_year; y<=year; y++) {
            for(String m : months) { values[c] = m + " " + y; c++; }
        }
        return values;
    }

    protected static int getYearMonthIdx(int year, int month) {
        return ((year-START_YEAR) * 12) + (month-1);
    }

    protected static ArrayList<Integer> getDateFromCode(String code) {
        ArrayList<Integer> dates = new ArrayList<>();

        try {
            DateFormat format = new SimpleDateFormat("MMM dd HH:mm:ss z yyyy");
            Date date = format.parse(code.substring(3).trim());
            dates = getDateFromTime(date.getTime());
            return dates;
        } catch (Exception e) {
            System.out.println(e);
        }
        return dates;
    }

    protected static ArrayList<Integer> getDateFromTime(Long time) {
        ArrayList<Integer> dates = new ArrayList<>();
        Calendar cal = Calendar.getInstance();
        Long t = (time==null) ? DEFAULT_TIME : time;
        cal.setTimeInMillis(t);
        dates.add(cal.get(Calendar.YEAR));
        dates.add(cal.get(Calendar.MONTH));
        return dates;
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="MySQL DB/Table Management">

    public static boolean databaseExist() throws Exception {
        return DBUtil.getInstance().checkDBExists(null);
    }

    public static void createDatabase() throws Exception {
        DBUtil.getInstance().createDatabase(null);
    }

    public static boolean tablesExist() throws Exception {
        boolean exists = false;

        try {
            int tblCount = 0;
            ResultSet rs = DBUtil.getInstance().getMysqlConnection().getMetaData().getTables(null, null, "%", null);
            while (rs.next()) {
                if (TABLES.contains(rs.getString(3))) { tblCount++; }
            }
            exists = (tblCount==TABLES.size());
        } catch (Exception e) {
            throw(e);
        }

        return exists;
    }
    
    public static String itemSave(String table){
         String itemSql ="INSERT INTO "+ table+" (`id`, `title`, `type`, `price`) VALUES "; 
         
            for(int i=0;i<50;i++)
             {  
               
                
                if(i==49){
                     itemSql += "(NULL,'title"+i+"', 'type"+i+"', '100"+i+"')";
                }
                else
                {
                    itemSql += "(NULL,'title"+i+"', 'type"+i+"', '100"+i+"'), ";
 
                }
               
             }

         return itemSql;
    }
          
 
   public static String makeFriends(int numberOfPeople)
   {
       int y =0;
       int x = 0;
       
      String sql = "INSERT INTO "+ TABLE_FRIENDS+" (`id`, `person_id`, `friend_id`) VALUES  ";
      
      for(int i=0;i<numberOfPeople;i++)
      {
        
          x = y+2;
          
          if(i==numberOfPeople-1)
          {
               int t = numberOfPeople-1;
               sql+= "(NULL,"+numberOfPeople+","+t+");";
          }
          else
          if(i%2!=0)
          {
              
              sql+= "(NULL,"+y+","+x+"),";
             
          }
          
           y++;
      }
      
       return sql;
   }

    public static void createTables(Boolean fillTables) throws Exception {
        try {
            String time = "year INTEGER, month INTEGER";
            String common = "title VARCHAR(60),type VARCHAR(60),price INT(11) ";
            String person = "firstname VARCHAR(50),lastname VARCHAR(50),address VARCHAR(50),phonenumber VARCHAR(50) ";
            String gadget ="itemname VARCHAR(50),type VARCHAR(50),price INT(11)";
            String transaction = "personId VARCHAR(50),itemId VARCHAR(50),price INT(11) ";
            
         //String 
           
           

            DBUtil.getInstance().createTable(TABLE_MOVIE, common);
            DBUtil.getInstance().createTable(TABLE_GADGET, gadget);
            DBUtil.getInstance().createTable(TABLE_GAME, common);
            DBUtil.getInstance().createTable(TABLE_BOOK,common);
            DBUtil.getInstance().createTable(TABLE_PERSON,person);
            DBUtil.getInstance().createTable(TABLE_TRANSACTION,transaction);
            
          String sql = "INSERT INTO "+ TABLE_PERSON+" (`id`, `firstname`, `lastname`, `address`, `phonenumber`) VALUES ";
          String gadgetSql =  "INSERT INTO "+ TABLE_GADGET+" (`id`, `itemname`, `type`, `price`) VALUES ";
         
            for(int i=0;i<50;i++)
             {  
               
                
                if(i==49){
                     sql += "(NULL,'firstname"+i+"', 'lastname"+i+"', 'P.O. Box 100"+i+"','024600582"+i+"')";
                     gadgetSql+="(NULL,'itemname"+i+"', 'type"+i+"', '100"+i+"')";

                     
                }
                else
                {
                    sql += "(NULL,'firstname"+i+"', 'lastname"+i+"', 'P.O. Box 100"+i+"','024600582"+i+"'), ";
                    gadgetSql+="(NULL,'itemname"+i+"', 'type"+i+"', '100"+i+"'),";
                }
               
             }

           
        DBUtil.getInstance().runSQLUpdate(sql);
        DBUtil.getInstance().runSQLUpdate(itemSave(TABLE_BOOK));
        DBUtil.getInstance().runSQLUpdate(itemSave(TABLE_MOVIE));
        DBUtil.getInstance().runSQLUpdate(itemSave(TABLE_GAME));
        DBUtil.getInstance().runSQLUpdate(gadgetSql);
        DBUtil.getInstance().runSQLUpdate(makeFriends(100));
        
        
        
        

          // if (fillTables) { BIDataManager.getInstance().update(); }
        } catch (SQLException e) {
            throw(e);
        } catch (Exception e) {
            throw(e);
        }
    }

    // </editor-fold>
}
