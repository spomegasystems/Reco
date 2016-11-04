/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.spomega.reco.domains.main;

import com.spomega.reco.domains.commons.GeneralInterface;
import com.spomega.reco.domains.commons.Generalimpl;
import com.spomega.reco.domains.commons.Status;
import org.neo4j.graphdb.Node;

/**
 *
 * @author Joseph George Davis
 * @date Nov 4, 2016 3:49:13 PM
 * description:
 */
public class Book extends Status implements GeneralInterface {

     Node underlyingNode;
    public Book(Node node) {
        super(node);
        underlyingNode = node;
    }

}
