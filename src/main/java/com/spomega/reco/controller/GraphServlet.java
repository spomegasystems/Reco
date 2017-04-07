/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.spomega.reco.controller;

import com.spomega.reco.domains.commons.Generalimpl;
import com.spomega.reco.domains.main.Book;
import com.spomega.reco.domains.main.Electronic;
import com.spomega.reco.domains.main.Game;
import com.spomega.reco.domains.main.Movie;
import com.spomega.reco.domains.main.Person;
import com.spomega.reco.models.PersonModel;
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
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

/**
 *
 * @author grameen
 */
@WebServlet(name = "GraphServlet", urlPatterns = {"/GraphServlet"})
public class GraphServlet extends HttpServlet {
 GraphDatabaseService database = DBUtil.getInstance().getGraphDB();
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
             Node person;
//             Node movie ;
//             Node book ;
//             Node electronic ;
//             
//             Node game ;
            
             
//             for(int i =0;i<100000;i++)
//             {
//             person = database.createNode(Labels.PERSON);
//            // movie  = database.createNode(Labels.MOVIE);
////             book  = database.createNode(Labels.BOOK);
////             electronic  = database.createNode(Labels.ELECTRONIC);
////             game  = database.createNode(Labels.GAME);
//             
//             
//              person.setProperty(Person.FIRSTNAME,"person"+i);
//              person.setProperty(Person.LASTNAME,"lastname"+i);
//              
////              movie.setProperty(Movie.TITLE, "movie"+i);
////               if(i%2==0)
////              movie.setProperty(Movie.TYPE, "drama "+i);
////              else
////                  movie.setProperty(Movie.TYPE, "sci-fi"+i);  
//              
//              //book.setProperty(Book.TITLE, "book"+i);
////              if(i%2==0)
////              book.setProperty(Book.TYPE, "fiction"+i);
////              else
////                  book.setProperty(Book.TYPE, "non-fiction"+i); 
////              
////              
////              electronic.setProperty(Electronic.TYPE, "gadget"+i);
////              
////              
////              game.setProperty(Game.TITLE, "game"+i);
////              game.setProperty(Game.TYPE, "adventure"+i);
//              
//              
//             
//              
//              //person.createRelationshipTo(movie,RecoRelationshipTypes.BUYS);
////              person.createRelationshipTo(book,RecoRelationshipTypes.BUYS);
////              person.createRelationshipTo(electronic,RecoRelationshipTypes.BUYS);
////              person.createRelationshipTo(game,RecoRelationshipTypes.BUYS);
//
//                 System.out.println("person " + i + "done");
//             
            // } 
             
             
             
            tx.success();
              
            
             
          
           }
           
           makeMovies(100000);
           
       // makeFriends(100000);
           
           
        }
    }
    
    
   // public void createRel(Node n)
            
    

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
    
    
    public void makeFriends(int number)
    {
      PersonModel personModel =  new PersonModel();
      try(Transaction tx = DBUtil.getInstance().getGraphDB().beginTx() )
       {
       
        for(int i=0;i<number-2;i++)
        {
             
            personModel.getPersonByName("person0").getUnderlyingNode().createRelationshipTo(personModel.getPersonByName("person"+(i+2)).getUnderlyingNode(),RecoRelationshipTypes.IS_A_FRIEND);
        }
         
        
        
        tx.success();
             
          
       }
           
    }
    
    public void makeMovies(int number)
    {
      PersonModel personModel =  new PersonModel();
       Node movie ;
      try(Transaction tx = DBUtil.getInstance().getGraphDB().beginTx() )
       {
        for(int i=0;i<number-2;i++)
        {
            movie  = database.createNode(Labels.MOVIE);
              movie.setProperty(Movie.TITLE, "movie"+i);
               if(i%2==0)
              movie.setProperty(Movie.TYPE, "drama "+i);
              else
             movie.setProperty(Movie.TYPE, "sci-fi"+i); 
            personModel.getPersonByName("person"+i).getUnderlyingNode().createRelationshipTo(movie,RecoRelationshipTypes.BUYS);
             tx.success();
        }
         
        
        
        tx.success();
             
          
       }
           
    }

}
