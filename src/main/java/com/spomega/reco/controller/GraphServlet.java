/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.spomega.reco.controller;

import com.spomega.reco.domains.commons.Generalimpl;
import com.spomega.reco.domains.main.Movie;
import com.spomega.reco.domains.main.Person;
import com.spomega.repo.util.DBUtil;
import com.spomega.repo.util.Labels;
import com.spomega.repo.util.RecoRelationshipTypes;
import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

/**
 *
 * @author grameen
 */
@WebServlet(name = "GraphServlet", urlPatterns = {"/GraphServlet"})
public class GraphServlet extends HttpServlet {

    /**
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code>
     * methods.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/html;charset=UTF-8");
        try (PrintWriter out = response.getWriter()) {
            /* TODO output your page here. You may use following sample code. */
           try(Transaction tx = DBUtil.getInstance().getGraphDB().beginTx() )
           {
//             Node person = DBUtil.getInstance().getGraphDB().createNode(Labels.PERSON);
//             Node movie  = DBUtil.getInstance().getGraphDB().createNode(Labels.MOVIE);
//             Node book  = DBUtil.getInstance().getGraphDB().createNode(Labels.BOOK);
//             Node electronic  = DBUtil.getInstance().getGraphDB().createNode(Labels.ELECTRONIC);
//             Node game  = DBUtil.getInstance().getGraphDB().createNode(Labels.GAME);
//             
             
             for(int i =0;i<50;i++)
             {
             Node person = DBUtil.getInstance().getGraphDB().createNode(Labels.PERSON);
             Node movie  = DBUtil.getInstance().getGraphDB().createNode(Labels.MOVIE);
             Node book  = DBUtil.getInstance().getGraphDB().createNode(Labels.BOOK);
             Node electronic  = DBUtil.getInstance().getGraphDB().createNode(Labels.ELECTRONIC);
             Node game  = DBUtil.getInstance().getGraphDB().createNode(Labels.GAME);
             
             
              person.setProperty(Person.FIRSTNAME,"person"+i);
              person.setProperty(Person.LASTNAME,"lastname"+i);
              
              movie.setProperty(Movie.TITLE, "movie"+i);
              movie.setProperty(Movie.TYPE, "drama "+i);
              
              person.createRelationshipTo(movie,RecoRelationshipTypes.BUYS);
              
             } 
             
             tx.success();
             
          
           }
        }
    }

    // <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /**
     * Handles the HTTP <code>GET</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /**
     * Handles the HTTP <code>POST</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /**
     * Returns a short description of the servlet.
     *
     * @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
        return "Short description";
    }// </editor-fold>

}
