/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.spomega.reco.domains.main;

import com.spomega.reco.domains.commons.GeneralInterface;
import com.spomega.reco.domains.commons.Status;
import static com.spomega.reco.domains.main.Person.LASTNAME;
import org.neo4j.graphdb.Node;

/**
 *
 * @author Joseph George Davis
 * @date Nov 4, 2016 3:54:47 PM
 * description:
 */
public class Movie extends Status implements GeneralInterface{
    
   
    Node underlyingNode;
    public Movie(Node underlyingNode) {
        super(underlyingNode);
        this.underlyingNode = underlyingNode;
    }
    
    

            
            
            

}
