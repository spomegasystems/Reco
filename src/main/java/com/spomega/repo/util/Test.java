/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.spomega.repo.util;

import static com.spomega.repo.util.BIUtil.TABLE_PERSON;

/**
 *
 * @author spomega
 */
public class Test {
    
    
    public static void main(String[] args) {
        
        
        
             for(int i=0;i<50;i++)
             {  
               
                String sql = "INSERT INTO "+ TABLE_PERSON+" (`id`, `firstname`, `lastname`, `address`, `phonenumber`) VALUES "
            + "('', 'firstname'"+i+", 'lastname'"+i+", 'P.O. Box 100'"+i+",'024600582'"+i+")";
                
                 System.out.println(sql);
             }
        
    }
    
}
 
             
