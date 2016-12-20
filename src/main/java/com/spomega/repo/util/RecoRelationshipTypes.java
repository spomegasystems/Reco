/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.spomega.repo.util;

import org.neo4j.graphdb.RelationshipType;

/**
 *
 * @author Joseph George Davis
 * @date Jul 16, 2015 10:49:08 AM
 * @description-
 */
public enum RecoRelationshipTypes implements RelationshipType
{

    //A
     AGENT,
     
     //C
     CREATED_BY,
     
     //B
     BUYS,
     
     
     //L
     LIKES,
     LAST_MODIFIED_BY,
     
    //I
     IS_A_FRIEND,
     
     
    //E
    ENTITY,
    
    //F
 
    //H
   
}
