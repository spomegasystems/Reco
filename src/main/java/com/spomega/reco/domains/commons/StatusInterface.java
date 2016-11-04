/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.spomega.reco.domains.commons;

/**
 *
 * @author Spomega
 * @Date Mar 6, 2015
 * @Email spomegasys@gmail.com
 * @Description- general interface for  status on all entities
 */
public interface StatusInterface extends GeneralInterface {
    
    
    public void setStatus(String status);
    
    public String getStatus();

}
