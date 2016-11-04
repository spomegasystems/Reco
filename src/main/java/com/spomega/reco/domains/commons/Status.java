/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.spomega.reco.domains.commons;

import org.neo4j.graphdb.Node;

/**
 *
 * @author Spomega
 * @Date Mar 6, 2015{time}
 * @Email spomegasys@gmail.com
 * @Description 
 */
public class Status extends Generalimpl implements  StatusInterface{
    
    
     public static String STATUS = "status";
    
    Node underlyingNode;

    public Status(Node underlyingNode) {
        super(underlyingNode);
        this.underlyingNode = underlyingNode;
    }
    
    
    @Override
    public void setStatus(String status) {
        System.out.println("the status is: "+ status);
        underlyingNode.setProperty(STATUS, status);
    }

    @Override
    public String getStatus() {
        try {
            return (String)underlyingNode.getProperty(STATUS);
        } catch (Exception e) { 
        }
        return ""; 
    }


}
