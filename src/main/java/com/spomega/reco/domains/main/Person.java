/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.spomega.reco.domains.main;

import com.spomega.reco.domains.commons.GeneralInterface;
import com.spomega.reco.domains.commons.Status;
import org.neo4j.graphdb.Node;

/**
 *
 * @author Joseph George Davis
 * @date Nov 4, 2016 3:11:07 PM
 * description:
 */
public class Person extends Status implements GeneralInterface{

    
    public static String FIRSTNAME = "firstname";
    public static String LASTNAME = "lastname";
    public static String ADDRESS  ="address";
    public static String PHONENUMBER ="phonenumber";
    
   
    
   
    Node underlyingNode;
    public Person(Node node) {
        super(node);
        underlyingNode = node;
         
          
    }

   
     
     
    public String getFirstName() {
         try {
            return (String) underlyingNode.getProperty(FIRSTNAME);

        } catch (Exception e) {
        }
        return "";
    }

    public void setFirstName(String firstName) {
          underlyingNode.setProperty(FIRSTNAME, firstName);
    }

    public String getLastName() {
          try {
            return (String) underlyingNode.getProperty(LASTNAME);

        } catch (Exception e) {
        }
        return "";
    }

    public void setLastName(String lastName) {
      underlyingNode.setProperty(LASTNAME, lastName);
    }

    public String getAddress() {
          try {
            return (String) underlyingNode.getProperty(ADDRESS);

        } catch (Exception e) {
        }
        return "";
    }

    public void setAddress(String address) {
        underlyingNode.setProperty(ADDRESS, address);
    }

    public String getPhoneNumber() {
        try {
            return (String) underlyingNode.getProperty(PHONENUMBER);

        } catch (Exception e) {
        }
        return "";
    }

    public void setPhoneNumber(String phoneNumber) {
        underlyingNode.setProperty(PHONENUMBER, phoneNumber);
    }
    
    
    
  
}
