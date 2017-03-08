/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.spomega.repo.util;

import static com.spomega.repo.util.BIUtil.TABLE_FRIENDS;
import static com.spomega.repo.util.BIUtil.TABLE_PERSON;

/**
 *
 * @author spomega
 */
public class Test {
    
    
    public static void main(String[] args) {
        
        int x= 0;
        int y = 0;
         String sql = "INSERT INTO "+ TABLE_FRIENDS+" (`id`, `person_id`, `friend_id`) VALUES  ";
        
       for(int i=0;i<100;i++)
      {
          y =1;
          x = y+2;
          
          if(i==(100-1))
          {
               int t = 100-1;
               sql+= "(NULL,"+100+","+t+");";
          }
          if(i%2!=0)
          {
              
              sql+= "(NULL,"+i+","+x+"),";
             
          }
          
         
      }
        
    }
    
}
 
             
