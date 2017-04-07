/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.spomega.repo.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.WrappingNeoServerBootstrapper;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ServerConfigurator;
/**
 *
 * @author Joseph George Davis
 * @date Sep 18, 2016 3:49:13 PM
 * description:
 */
public class DBUtil {
    
    
 static   Logger log = Logger.getLogger(DBUtil.class.getName());
    
   
    protected static String PORT = null;
    protected static String DATABASE_PATH = null;
    protected static GraphDatabaseService database = null;
     static WrappingNeoServerBootstrapper srv;
    static String SERVER_HOSTNAME = "0.0.0.0";

    private final static String MYSQL_DB = "recodb";
    private final static String MYSQL_PORT = "3306";
    private final static String MYSQL_HOST = "localhost";
    private final static String MYSQL_USER = "root";
    private final static String MYSQL_PASS = "spomega";
    private final static String MYSQL_DSN = "jdbc:mysql://"+MYSQL_HOST+":"+MYSQL_PORT+"/"+MYSQL_DB;
    private static Connection connection = null;
    
    private static DBUtil instance =  new DBUtil();
    
    
    private DBUtil(){}
    
    public static DBUtil getInstance ()
    {
        return  instance;
    }
    
      // <editor-fold defaultstate="collapsed" desc="Neo4j DB Methods">
      public GraphDatabaseService startDB() {
        GraphDatabaseService graphdb = null;
        String glassishinstanceRootPropertyName = "com.sun.aas.instanceRoot";
        String configFile =  System.getProperty(glassishinstanceRootPropertyName)+"/config/reco.conf";
        System.out.println("File " + configFile);
      
        Properties dbProperties = new Properties();
        
        try(InputStream inputStream = new FileInputStream(configFile))
        {
            //load properties file
            dbProperties.load(inputStream);
        } catch (FileNotFoundException ex) {
           //Logger.getLogger(DatabaseUtil.class.getName()).log(Level.ERROR, null, ex);
            System.out.println("File not found " + ex.getLocalizedMessage());
            ex.printStackTrace();
        }
        catch(Exception ex)
        {
          System.out.println("File not found " + ex.getLocalizedMessage());
          ex.printStackTrace();
        }
        
        try {
            DATABASE_PATH = dbProperties.getProperty("graphdb.location");
            PORT = dbProperties.getProperty("graphdb.port");
            log.log(Level.INFO, "DB Path - :{0}", DATABASE_PATH);
            log.log(Level.INFO, "DB Port - :{0}", PORT);

            graphdb = new GraphDatabaseFactory()
                    .newEmbeddedDatabaseBuilder( DATABASE_PATH)
                    .setConfig(GraphDatabaseSettings.allow_store_upgrade, dbProperties.getProperty("allow_store_upgrade"))
                    .setConfig(GraphDatabaseSettings.pagecache_memory, dbProperties.getProperty("dbms.pagecache.memory"))
                    .newGraphDatabase();
        } catch (Exception e) {
            log.log(Level.SEVERE, "DS Error : {0}", e.getLocalizedMessage());
            e.printStackTrace();
        }
        
        database = graphdb;

        registerShutdownHook(graphdb);
        log.log(Level.INFO, "{0} Database test 2 done", graphdb.isAvailable(2));
// 
//        ServerConfigurator  config = new ServerConfigurator((GraphDatabaseAPI) graphdb);
//                 
//         config.configuration().setProperty(
//         Configurator.WEBSERVER_PORT_PROPERTY_KEY,"9494");
//         config.configuration().setProperty(Configurator.WEBSERVER_ADDRESS_PROPERTY_KEY, SERVER_HOSTNAME);
//         config.configuration().setProperty(Configurator.HTTP_LOGGING,true);
//         config.configuration().setProperty(Configurator.HTTP_CONTENT_LOGGING,true);
//         
//
//        srv = new WrappingNeoServerBootstrapper((GraphDatabaseAPI) graphdb, config);
//        
//        srv.start();
       log.log(Level.INFO,"i am alive here");
       return  graphdb;
    }

    public GraphDatabaseService getGraphDB() {
        return (database==null) ? startDB() : database;
    }
    
    private static void registerShutdownHook(final GraphDatabaseService graphDb) {
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                try {
                    log.info("shutting down my db");
                    graphDb.shutdown();
                }
                catch(Exception e)
                {
                    System.out.println("    "+ e.getMessage());
                    e.printStackTrace();
                }
            }
        });
    }
    
    public void shutdown(final GraphDatabaseService graphDb) {
        try {
            graphDb.shutdown();
        } catch (Exception e) {
            log.log(Level.WARNING, "Shut Down Thread {0}", e.getLocalizedMessage());
            e.printStackTrace();
        }
    }
    
     // </editor-fold>
    
      // <editor-fold defaultstate="collapsed" desc="Mysql DB Methods">

    public void startMysqlDB() throws SQLException {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(MYSQL_DSN,MYSQL_USER, MYSQL_PASS);
        } catch (ClassNotFoundException e) {
            System.out.println("Connection Failed! Class com.mysql.jdbc.Driver not found");
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            throw(e);
        }
    }

    public Connection getMysqlConnection() throws SQLException {
        try {
            if (connection==null) {
                startMysqlDB();
            }
            return connection;
        } catch (SQLException e) {
            throw(e);
        }
    }

    public void closeMysqlConnection() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            System.out.println("Closing connection Failed! Check output console");
            e.printStackTrace();
        }
    }

    public void createTable(String tableName, String fields) throws SQLException {
        try {
           String sql = "CREATE TABLE IF NOT EXISTS " + tableName + "(id int(11) NOT NULL AUTO_INCREMENT, "
                        + fields + ", PRIMARY KEY (`id`))";
            Statement stmt = getMysqlConnection().createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
           System.out.println("Error creating table "+tableName);
           throw(e);
        }
    }

    public void emptyTable(String tableName) throws SQLException {
        try {
            runSQLUpdate("TRUNCATE TABLE " + tableName);
        } catch (SQLException e) {
            System.out.println("Error emptying table "+tableName);
            throw(e);
        }
    }

    public boolean checkDBExists(String dbName) throws SQLException {
        try{
            dbName = (dbName==null) ? MYSQL_DB : dbName;
            ResultSet resultSet = getMysqlConnection().getMetaData().getCatalogs();
            while (resultSet.next()) {
                String databaseName = resultSet.getString(1);
                if(databaseName.equals(dbName)){
                    return true;
                }
            }
            resultSet.close();
        }
        catch(SQLException e){
            throw(e);
        }

        return false;
    }

    public void createDatabase(String name) throws SQLException {
        try {
            name = (name==null) ? MYSQL_DB : name;
            String sql = "CREATE DATABASE " + name;
            Statement stmt = getMysqlConnection().createStatement();
            stmt.execute(sql);
        } catch(SQLException e) {
            throw(e);
        }
    }

    public ResultSet runSQLSelect(String sql) throws SQLException {
        try {
            Statement stmt = getMysqlConnection().createStatement();
            return stmt.executeQuery(sql);
        } catch(SQLException e) {
            throw(e);
        }
    }

    public int runSQLUpdate(String sql) throws SQLException {
        try {
            System.out.println("SQL: "+sql);
            Statement stmt = getMysqlConnection().createStatement();
            return stmt.executeUpdate(sql);
        } catch(SQLException e) {
            throw(e);
        }
    }


    // </editor-fold>

}
