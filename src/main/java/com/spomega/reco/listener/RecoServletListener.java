/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.spomega.reco.listener;

import com.spomega.repo.util.BIUtil;
import com.spomega.repo.util.DBUtil;
import com.spomega.repo.util.Neo4jServices;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

/**
 * Web application lifecycle listener.
 *
 * @author grameen
 */
public class RecoServletListener implements ServletContextListener {

    @Override
    public void contextInitialized(ServletContextEvent sce) {
    
         System.out.println("-----------------------------Starting Graph Database-------------------------------------");
          System.out.println("database  " + DBUtil.getInstance().startDB() ); 

        //checks if root node exists
//        try (Transaction tx =   DBUtil.getInstance().getGraphDB().beginTx()) {
//              if(Neo4jServices.getRootNode()) {
//                  System.out.println("Root Node Already Exists");
//              }
//              else
//              {
//                  Node node =   DBUtil.getInstance().getGraphDB().createNode(DynamicLabel.label("root"));
//                  node.setProperty("name", "ICTCROOT");
//                  System.out.println("Node Added");
//                  System.out.println("node Added" + node.getId());
//                  System.out.println("node Added" + node.getProperty("name"));
//                  tx.success();
//              }
//        } catch (Exception e) {
//                  System.out.println("Unable to create root node");
//                  e.printStackTrace();
//        }
        System.out.println("-----------------------------GraphDB Started-------------------------------------");


        System.out.println("-----------------------------Initializing MySQL Database-------------------------------------");
        try {
            //  DBUtil.getInstance().startMysqlDB();
            //if (!BIServices.databaseExist()) { BIServices.createDatabase(); }
          // 
             // if (!BIUtil.tablesExist()) { BIUtil.createTables(true); }
        } catch (Exception e) {
            System.out.println("Unable to initialize MySQL DB");
            e.printStackTrace();
        }
        System.out.println("-----------------------------MySQL Database Initialized-------------------------------------");
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        DBUtil.getInstance().shutdown(DBUtil.getInstance().getGraphDB());
    }
}
