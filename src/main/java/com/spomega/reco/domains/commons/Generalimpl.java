/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.spomega.reco.domains.commons;



import static com.spomega.reco.domains.main.Movie.TITLE;
import static com.spomega.reco.domains.main.Movie.TYPE;
import com.spomega.repo.util.ICTCUtil;
import com.spomega.repo.util.Neo4jServices;
import com.spomega.repo.util.RecoKonstants;
import com.spomega.repo.util.RecoRelationshipTypes;
import java.util.Date;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 *
 * @author Spomega
 * @Date Mar 6, 2015{time}
 * @Email spomegasys@gmail.com
 * @Description 
 */
public class Generalimpl implements GeneralInterface {
    
    public static String CREATION_DATE = RecoKonstants.CREATED_ON;
    public static String ID = RecoKonstants.ID;
    public static String CREATION_BY = RecoKonstants.CREATED_BY;
    public static String LAST_MODIFIED = RecoKonstants.LAST_MODIFIED_DATE;
    public static String UPDATED_BY = RecoKonstants.UPDATED_BY;
    public static String TITLE = "title";
    public static String TYPE = "type";
    
    Node underlyingNode = null;

    public Generalimpl(Node node) {
        
        this.underlyingNode = node;
        
        
    }
    
    
    public String getTitle() {
        try {
            return (String) underlyingNode.getProperty(TITLE);

        } catch (Exception e) {
        }
        return "";
    }

    public void setTitle(String title) {
          underlyingNode.setProperty(TITLE, title);
    }

    public String getType() {
         try {
            return (String) underlyingNode.getProperty(TYPE);

        } catch (Exception e) {
        }
        return ""; 
    }

    public void setType(String type) {
          underlyingNode.setProperty(TYPE, type);
    }
    
    
     @Override
    public void setCreatedOn(Date creationDate) {
        underlyingNode.setProperty(CREATION_DATE,
                ICTCUtil.dateToLong(creationDate));
    }

    @Override
    public Date getCreatedOn() {
        try {
            return  ICTCUtil.LongToDate((Long) underlyingNode.getProperty(CREATION_DATE));
        } catch (Exception e) {
        }
        return null;

    }

    @Override
    public void setLastModifiedDate(Date lastModificationDate) {
        underlyingNode.setProperty(LAST_MODIFIED,   ICTCUtil.dateToLong(lastModificationDate));
    } 
    
    
    @Override
    public void setLastModifiedDate(long lastModificationDate) {
        underlyingNode.setProperty(LAST_MODIFIED,  (lastModificationDate));
    }

    @Override
    public Date getLastModifiedDate() {
        return   ICTCUtil.LongToDate((Long) getUnderlyingNode().getProperty(LAST_MODIFIED));
    }

    @Override
    public Node getLastModifiedBy() {
       Iterable<Relationship> relationships = 
                underlyingNode.getRelationships(Direction.OUTGOING, RecoRelationshipTypes.LAST_MODIFIED_BY);
        
         for (Relationship relationship : relationships) {
             return relationship.getEndNode();
         }
        return null;
    }

    @Override
    public void setLastModifiedBy(Node updatedBy) {
        try {
            Neo4jServices.deleteRelationship(updatedBy,  RecoRelationshipTypes.LAST_MODIFIED_BY);
        } catch (Exception e) {
        }
        underlyingNode.createRelationshipTo(updatedBy,RecoRelationshipTypes.LAST_MODIFIED_BY);
    }

    public Node getUnderlyingNode() {
        return underlyingNode;
    }

    @Override
    public void setId(String id) {
        underlyingNode.setProperty(ID, id);

    }

    @Override
    public String getId() {

        try {
            return (String) underlyingNode.getProperty(ID);
        } catch (Exception e) {
        }

        return null;
    }

    
     @Override
    public Node getCreatedBy() {
//        Iterable<Relationship> relationships = 
//                underlyingNode.getRelationships(Direction.OUTGOING, KnoxxiRelationshipType.CREATED_BY);
//        
//         for (Relationship relationship : relationships) {
//             return relationship.getEndNode();
//         }
//   
//         
         return null;
     //return NeoUtil.findRelationFromNode(underlyingNode, Direction.OUTGOING, KnoxxiRelationshipType.CREATED_BY); 
    }

    @Override
    public void setCreatedBy(Node created) {
        try {
             Neo4jServices.deleteARelation(created, RecoRelationshipTypes.CREATED_BY);
        } catch (Exception e) {
        }
        underlyingNode.createRelationshipTo(created, RecoRelationshipTypes.CREATED_BY);
    }

   

   


    
    
    
    
    
    

}
