/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.spomega.repo.util;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
//import org.neo4j.kernel.impl.util.StringLogger;
import scala.collection.Iterator;

/**
 *
 * @author Spomega
 * @Date Mar 6, 2015
 * @Email jdavis@grameenfoundation.org
 * @Description
 */
public class Neo4jServices {

    private final static Logger log = Logger.getLogger(Neo4jServices.class.getName());
    static GraphDatabaseService db =  DBUtil.getInstance().getGraphDB();
    static java.util.logging.Logger logg = java.util.logging.Logger.getLogger(Neo4jServices.class.getName());

    /**
     *
     * @param node
     * @param relations
     * @return
     */
    public static boolean deleteRelationship(Node node, RelationshipType relations) {
        // Begin the transaction

        try (Transaction trx = db.beginTx()) {
            // Deletes the relations of the node
            Iterable<Relationship> relationships = node.getRelationships();

            for (Relationship relationship : relationships) {

                if (relationship.getType().equals(relations)) {
                    relationship.delete();
                    // log.warn("Deleted : " + relationship.getType());
                } else {
                    log.info("Relationship Not deleted : " + relationship.getType());
                }
            }
            trx.success();
            return true;
        } catch (Exception e) {
            // When an error occur
            e.printStackTrace();
            log.info("tx.failure() [Delete Rel] : " + e);
        }

        return false;
    }

    public static boolean deleteARelation(Node node, RelationshipType relations) {
        return deleteARelation(node, relations, Direction.OUTGOING);
    }

    public static boolean deleteARelation(Node node, RelationshipType relations, Direction direct) {

        // log.warn("Relation to be deleted : "+relations);
        // Begin the transaction
        try (Transaction trx = db.beginTx()) {
            // Deletes the relations of the node
            String q = "";
            if (direct.equals(Direction.OUTGOING)) {
                q = "start n=node(" + node.getId() + ") match n-[r:" + relations + "]->s"
                        + " delete r ";
            } else if (direct.equals(Direction.INCOMING)) {
                q = "start n=node(" + node.getId() + ") match n<-[r:" + relations + "]->s"
                        + " delete r ";

            } else {
                q = "start n=node(" + node.getId() + ") match n<-[r:" + relations + "]->s"
                        + " delete r ";
            }
            log.info("Query : qu ");
            executeCypherQuery(q);
            trx.success();
            return true;
        } catch (Exception e) {
            // When an error occurs
            log.info("deleting Relationship failed");
        }

        return false;
    }
    
    
    /**
     *
     * This function creates a new reference Node to the root node
     *
     * @param refNode
     */
    public static Node createReferenceNode(String refNode) {
      
        Node n = null;
     
          try (Transaction tx = DBUtil.getInstance().getGraphDB().beginTx()) {
               
             Node neoNode =null;
            //Get the root Node
            Node rootNode = executeCypherQuerySingleResult("start rt=node(0) return rt", "rt");

            //  System.out.println("root node " + rootNode.getId());
            //Create the new node
             neoNode = db.createNode(Labels.PARENT);
            //Set the name for that node
            neoNode.setProperty(RecoKonstants.NAME, refNode);

            //relate the new node to the parent node
            rootNode.createRelationshipTo(neoNode, RecoRelationshipTypes.ENTITY);
            
             
            // n = getReferenceNode(refNode);
             n = neoNode;
             tx.success();
            
             
        } catch (Exception e) {

            log.info("Error creating Reference Node");
            e.printStackTrace();
        }
       
      
      
        return n;
    }

    /**
     *
     * Executives a given cypher Query
     *
     * @param q the cypher query be executed
     * @return executionResult the result of execution of that query
     *
     *
     */
    public static Result executeCypherQuery(String q) {
        
        GraphDatabaseService db = DBUtil.getInstance().getGraphDB();
        Result result = null;
        // let's execute a query now
        try (Transaction tx = db.beginTx()) {
            //ExecutionEngine engine = new ExecutionEngine(
           // ICTCDBUtil.getInstance().getGraphDB(), StringLogger.SYSTEM);
            //System.out.println("execute cypher query");
            result = db.execute(q);
            tx.success();
        } catch (Exception e) {
            log.info("Error creating Executing Query : " + e.getLocalizedMessage());
        }
        return result;
    }

  

    /**
     * Returns a node this is the result of a given query
     *
     * @param q
     * @param column
     * @return
     */
    public static Node executeCypherQuerySingleResult(String q, String column) {
        // let's execute a query now
        //System.out.println("Get Single Instance");
        //Transaction tx = DataSource.getGraphDBAPI().beginTx();
        //GraphDatabaseService db =  ICTCDBUtil.getInstance().getGraphDB();
        Result result = null;
        try(Transaction tx = db.beginTx()) {
            //ExecutionEngine engine = new ExecutionEngine(
                    //ICTCDBUtil.getInstance().getGraphDB(), StringLogger.SYSTEM);
            //result = engine.execute(q);
            //  tx.success();
           result =  db.execute(q);
          tx.success();
        } catch (Exception e) {
            e.printStackTrace();
            //tx.failure();
            log.info("Error Executing Cypher Query : " + q);
            log.info("Exception : " + e);
       }
       // finally {
//            //tx.finish();
//        }
        //  log.warn("After Find");

        return nodeFromResult(result, column);
    }

    /**
     * This function gets the node form a query that only returns a single
     * result
     *
     * @param result
     * @param column
     * @return
     */
    public static Node nodeFromResult(Result result, String column) {

       
//		@SuppressWarnings("unchecked")
        
        ResourceIterator<Node> n_column = result.columnAs(column);
        while (n_column.hasNext()) {
            return n_column.next();
        }
        
        
        //  log.warn("Nothing found");
        return null;
    }

    /**
     * Get a reference node
     *
     * @param nodeName
     * @return
     */
    public static Node getReferenceNode(String nodeName) {
        
            
        return getReferenceNode(nodeName, false);
       
        
    }

    /**
     *
     * @param nodeName
     * @param recreateIfNotExist recreates the node if it does not exist
     * @return if true the node would be recreated when you not found and other
     * wise if false
     */
    public static Node getReferenceNode(String nodeName, boolean recreateIfNotExist) {
        
        
      try(Transaction tx = db.beginTx()) {
            Node n = null;
            try{
                
            String q = "Start root=node(0) " + " MATCH root-[:" + RecoRelationshipTypes.ENTITY + "]->e "
                    + " where e.name='" + nodeName + "' " + " return e";
             System.out.println("query " + q);
             Result result = executeCypherQuery(q);
             
            
            System.out.println("result " + result.columnAs("e"));
             
            ResourceIterator<Node> n_column = result.columnAs("e");
                
            while (n_column.hasNext()) {
                n = n_column.next();   
                
            }
            
          
            if (recreateIfNotExist && null==n) {
                System.out.println("Creating New Node from recreate");
                n = createReferenceNode(nodeName);
            }
          
            
          } catch (Exception e) {
            log.info("Unable to find " + nodeName);
            e.printStackTrace();

            log.info("Creating Node " + nodeName);
            if (recreateIfNotExist) {
                log.info("Creating New");
                n=createReferenceNode(nodeName);
            }
        }
             
        
          System.out.println("none " + n);
         tx.success();
          return n;
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Get a reference node
     *
     * @param nodeName
     * @return
     */
    public static Node getReferenceNodeCreatIfNotExist(String nodeName) {
     
        return getReferenceNode(nodeName, true);
    }

    public static Node findNodeFromRelation(Node underlyingNode, Direction direction, RecoRelationshipTypes relationType) {
        Iterable<Relationship> relationships = underlyingNode.getRelationships(direction, relationType);
        for (Relationship relationshp : relationships) {
            if (direction.equals(Direction.OUTGOING)) {
                return relationshp.getEndNode();
            } else {
                return relationshp.getStartNode();
            }
        }
        return null;
    }

    public static List<Node> findNodeFromRelations(Node underlyingNode, Direction direction, RecoRelationshipTypes relationType) {
        Iterable<Relationship> relationships = underlyingNode.getRelationships(direction, relationType);
        List<Node> n = new ArrayList<Node>();
        for (Relationship relationshp : relationships) {
            if (direction.equals(Direction.OUTGOING)) {
                n.add(relationshp.getEndNode());
            } else {
                n.add(relationshp.getStartNode());
            }
        }
        return n;
    }

  
    

    public static long getAggregatedValue(String q) {
       
        ResourceIterator<Long> n_column = null;
        Result result = null;

        db = DBUtil.getInstance().getGraphDB();

        try (Transaction tx = db.beginTx()) {
            //Neo4j 1.1.* implemention commented out
//           ExecutionEngine engine = new ExecutionEngine(
//           ICTCDBUtil.getInstance().getGraphDB(), StringLogger.SYSTEM);
//           result = engine.execute(q);
             result = db.execute(q);
             n_column = result.columnAs("l");
             while (n_column.hasNext())
             {
                    tx.success();
                   return n_column.next();
             }
             tx.success();
            return 0l;
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
        }

        return 00;
    }

    public static long getSumValue(String q) {
        ResourceIterator<Long> n_column = null;
        Result result = null;
        
       
        // let's execute a query now
        try (Transaction tx = db.beginTx()) {
            
              //Neo4j 1.1.* implemention commented out
//            ExecutionEngine engine = new ExecutionEngine(
//                    ICTCDBUtil.getInstance().getGraphDB(), StringLogger.SYSTEM);
//            result = engine.execute(q);
             
            result = db.execute(q);
            n_column = result.columnAs("l");
            if (!n_column.hasNext()) {
                return 0;
            }
            System.out.println("Not Emptty");
            while (n_column.hasNext()) {
                if (null == n_column.next()) {
                    return 0;
                }
                tx.success();
                return n_column.next();
            }
            tx.success();
           return 0;
        } catch (Exception e) {
            System.out.println("Exception SUm Values  -> " + e.getLocalizedMessage());
            return 0;
        }

       
    }

    public static Object getAggregateItem(String q) {
//        Iterator<Long> n_column = null;
       // org.neo4j.cypher.javacompat.ExecutionResult result = null;
        
        // let's execute a query now
        try (Transaction tx = DBUtil.getInstance().getGraphDB().beginTx()) {
           // org.neo4j.cypher.javacompat.ExecutionEngine engine = new org.neo4j.cypher.javacompat.ExecutionEngine(
                   // ICTCDBUtil.getInstance().getGraphDB(), StringLogger.SYSTEM);
           // result = engine.execute(q);
           
            Result result = db.execute(q);

            while (result.hasNext()) {
                Map<String, Object> row = result.next();
                for (String key : result.columns()) {
                    System.out.printf("%s = %s%n", key, row.get(key));
                    tx.success();
                    return row.get(key);
                }
            }
            
//            for (Map<String, Object> row : result) {
//                for (Map.Entry<String, Object> column : row.entrySet()) {
//                    rows += column.getKey() + ": " + column.getValue() + "; ";
//                    System.out.println("Rows : " + row);
//                    return column.getValue();
//                }
//                rows += "\n";
//            }
                 tx.success();
        } catch (Exception e) {
            System.out.println("Exception SUm Values  -> " + e.getLocalizedMessage());
            return 0;
        }

        
        return 0;
    }
    
    public static double getCollectionValue(String type, String label, String fieldName) {
        String q = " match(n:" + label + ") return " + type + "(toFloat(n." + fieldName + "))  as l ";
        System.out.println("Query : " + q);
        Object j = getAggregateItem(q);

        try {
            if (null != j) {
                String jk = j.toString();

                return Double.parseDouble(jk);
            }
        } catch (Exception e) {
        }
        return 0.0;

    }

    public static List<String> getIterativeString(String q) {
        ResourceIterator<String> n_column = null;
        List<String> bdata = new ArrayList<>();
        Result result = null;

        try (Transaction tx = db.beginTx()) {
//            ExecutionEngine engine = new ExecutionEngine(
//                    ICTCDBUtil.getInstance().getGraphDB(), StringLogger.SYSTEM);
            result = db.execute(q);

            n_column = result.columnAs("l");

            while (n_column.hasNext()) {

                String n = n_column.next();
                bdata.add(n);
            }
            tx.success();
        } catch (Exception e) {
            System.out.println("Error getIterativeString Executing Query : " + e.getLocalizedMessage());
            e.printStackTrace();
        }

        return bdata;
    }

    public static ResourceIterator<Node> executeIteratorQuery(String query, String returnItem) {

        try (Transaction tx = db.beginTx()) {
//            ExecutionEngine engine = new ExecutionEngine(
//                    ICTCDBUtil.getInstance().getGraphDB(), StringLogger.SYSTEM);
            Result result = executeCypherQuery(query);
            tx.success();
            return result.columnAs(returnItem);
           
        }

    }

    public static void executeVoidQuery(String query) {

        try (Transaction tx =db.beginTx()) {
//            ExecutionEngine engine = new ExecutionEngine(
//                    ICTCDBUtil.getInstance().getGraphDB(), StringLogger.SYSTEM);
           Result result = executeCypherQuery(query);
          tx.success();
        }

    }

    public static Node executeSingleQuery(String query, String returnItem) {

        try (Transaction tx = DBUtil.getInstance().getGraphDB().beginTx()) {

          Result result = executeCypherQuery(query);
            
            // ExecutionResult result = (ExecutionResult) ICTCDBUtil.getInstance().getGraphDB().execute(query);
            ResourceIterator<Node> n = result.columnAs(returnItem);
            while (n.hasNext()) {
                tx.success();
                return n.next();
            }
            tx.success();
            return null;
        }

    }

  

   
    
    public static boolean getRootNode()
    {
        
        String q = "match (n:root) return n";
        System.out.println("Query : " + q);
        
          try {
            Node node =executeCypherQuerySingleResult(q, "n");
              System.out.println("node " + node.getProperty("name"));
            if (null != node) {
                return true;
            }
        } catch (Exception e) {
            System.out.println("Unable to Find Root Node " + e.getMessage());
            e.printStackTrace();
        }
                 return false;
    }

}
