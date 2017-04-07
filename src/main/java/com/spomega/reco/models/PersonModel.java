/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.spomega.reco.models;

import com.spomega.reco.domains.main.Person;
import com.spomega.repo.util.Neo4jServices;
import org.neo4j.graphdb.Node;

/**
 *
 * @author Joseph George Davis
 * @date Dec 19, 2016 3:00:24 PM
 * description:
 */
public class PersonModel {
    
    
    
    public Person getPersonByName(String firstname)
    {
        
          String q = "MATCH (l:PERSON) WHERE l.firstname='"+firstname+"'  RETURN l ";

        System.out.println("Query " + q);
        try {
            Node node = Neo4jServices.executeCypherQuerySingleResult(q, "l");
            if (null != node) {
                return new Person(node);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Unable to Find Person");
        }
       

        return null;
        
    }

}
