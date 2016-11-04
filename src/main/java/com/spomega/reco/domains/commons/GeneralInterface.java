/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.spomega.reco.domains.commons;


import java.util.Date;
import org.neo4j.graphdb.Node;

/**
 *
 * @author Spomega
 * @Date Mar 6, 2015
 * @Email spomegasys@gmail.com
 * @Description-
 */
public interface GeneralInterface {
    
     public void setId(String id);

    public String getId();

    public Date getCreatedOn();

    public void setCreatedOn(Date createdOn);

    public Node getCreatedBy();

    public void setCreatedBy(Node createdBy);

   // public void setCreatedBy(UserImpl createdBy);
    
     public Date getLastModifiedDate();

    public void setLastModifiedDate(Date lastModifiedDate);
    
   
    public void setLastModifiedDate(long lastModifiedDate);
    

    public Node getLastModifiedBy();

    public void setLastModifiedBy(Node lastModifiedBy);
    
    //public void setLastModifiedBy(UserImpl lastModifiedBy);
    

}
