package com.spomega.repo.util;

import org.apache.commons.lang.WordUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by David on 8/19/2016.
 * Main interface file to Neo4j and MySQL data stores
 */
public class BIDataManager extends BIUtil {

    private final static String TAG = BIDataManager.class.getName();

    private String data_set;

    public static BIDataManager instance;

    // <editor-fold defaultstate="collapsed" desc="Constructors">
    public static BIDataManager getInstance() {
        instance = new BIDataManager(null);
        return instance;
    }

    public static BIDataManager getInstance(String data_set) {
        instance = new BIDataManager(data_set);
        return instance;
    }

    protected BIDataManager(String i) {
        data_set = (i==null) ? "all" : i;
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Get data methods">
   
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Update data methods">
 
    // </editor-fold>
}
