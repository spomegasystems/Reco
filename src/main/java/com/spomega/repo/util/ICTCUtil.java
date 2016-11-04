/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.spomega.repo.util;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

/**
 *
 * @author Joseph George Davis
 * @date Jul 16, 2015 11:31:27 AM description:
 */
public class ICTCUtil {

    public static final String GENERAL_RESPONSE = "generalResponse";
    public static final String ERROR = "error";
    public static final String SUCCESS = "success";

    //Converts date to timesramp

    public static long dateToLong(Date date) {
        if (date != null) {
            return date.getTime();
        } else {
            return 0l;
        }
    }

    
    public static String dt(String date) {
        if (date != null) {
           
        } else {
            return "";
        }
        
        return "";
    }

    
    
    public static Date LongToDate(long str) {

        if (str == 0l) {
            return null;
        }
        {
            Date t = new Date();
            t.setTime(str);
            return t;
        }
    }

    public static String replaceKeyInObject(String toBeReplace) {
        String[] keys = {"production", "identification", "expected", "renting", "number", "target", "renting", "application", "fertilizer", "herbicide", "renting", "applied", "quantity", "yield", "acre", "number", "land", "price", "ton", "of"};
        for (String key : keys) {
            toBeReplace = toBeReplace.replace(key, " " + key.substring(0, 1).toUpperCase() + key.substring(1));
            System.out.println("Rreplcea : " + toBeReplace);
        }
        return toBeReplace;

    }

    public static void redirect(HttpServletRequest request, HttpServletResponse response, String redirectTo, Map<String, String> errors) throws ServletException, IOException {
        String tmp = request.getHeader("referer").trim();

        //System.out.println("Tmp : " + tmp);
        String proc = request.getScheme();
        String server = request.getServerName();
        String context = request.getContextPath();
        String path = request.getServletPath();
        int port = request.getServerPort();

        HttpSession session = request.getSession();
        if (session.getAttribute("referer") == null) {
            session.setAttribute("referer", tmp);
            session.setAttribute("qstring", request.getQueryString());
        }

        String construct = proc + "://" + server + ":" + port + context;
        String full = construct + path;

        if (!tmp.equals(full)) {
            session.setAttribute("referer", tmp);
            session.setAttribute("qstring", request.getQueryString());
        }

        String queryString = (String) session.getAttribute("qstring");
        String referer = (String) session.getAttribute("referer");

        referer = referer.substring(construct.length() + 1);

        if (queryString != null) {
            referer += "?" + queryString;
        }

        request.setAttribute("error", errors);

        //System.out.println("Errors : " + referer);
        RequestDispatcher requestDispatcher;
        requestDispatcher = request.getRequestDispatcher(tmp);
        requestDispatcher.forward(request, response);

    }

    public static void redirect(HttpServletRequest request, HttpServletResponse response, String url, Object responseObj) throws IOException {

        ////System.out.println("Redirect To : " + redirectTo);
        HttpSession session = request.getSession();
        session.setAttribute(RecoKonstants.RESPONSE, "000");
        session.setAttribute("responseObject", responseObj);
        //  response.sendRedirect(redirectTo);

        try {
            String path = "";
            if (request.getHeader("referer") != null) {
                path = request.getHeader("referer");
            } else if (url != null || url == "") {
                path = url;
            } else {
                path = request.getContextPath();
            }

//            //request.setAttribute("PAGE_ID", getActionModel().actionByUrl(url));
            request.getRequestDispatcher(url).forward(request, response);
            return;
        } catch (Exception e) {
            //System.out.println("Error Occured : " + e.toString());
            e.printStackTrace();
            return;
        }
    }

    public static void doRedirect(String url, HttpServletRequest request, HttpServletResponse response, Map<String, String> errors) {
        try {
            String path = "";
            if (request.getHeader("referer") != null) {
                path = request.getHeader("referer");
            } else if (url != null || url == "") {
                path = url;
            } else {
                path = request.getContextPath();
            }
            try {
                //request.setAttribute("PAGE_ID", getActionModel().actionByUrl(url));
            } catch (Exception e) {
            }

            try {

                request.getSession().setAttribute("error", errors);
            } catch (Exception e) {
            }

            request.setAttribute("response", "001");
            request.getRequestDispatcher(url).forward(request, response);
            return;
        } catch (Exception e) {
            //System.out.println("Error Occured : " + e.toString());
            e.printStackTrace();
            return;
        }
    }

    public static void doSuccessRedirect(String url, HttpServletRequest request, HttpServletResponse response, Map<String, String> errors) {
        try {
            String path = "";
            if (request.getHeader("referer") != null) {
                path = request.getHeader("referer");
            } else if (url != null || url == "") {
                path = url;
            } else {
                path = request.getContextPath();
            }
            try {
                //request.setAttribute("PAGE_ID", getActionModel().actionByUrl(url));
            } catch (Exception e) {
            }

            request.setAttribute("response", "000");
            request.getRequestDispatcher(url).forward(request, response);
            return;
        } catch (Exception e) {
            //System.out.println("Error Occured : " + e.toString());
            e.printStackTrace();
            return;
        }
    }

    public static void doRRedirect(String url, HttpServletRequest request, HttpServletResponse response, Map<String, String> errors) {
        try {
//           // log.warn("do RRdirect ");
            request.getSession().setAttribute("error", errors);
            request.getSession().setAttribute("response", "001");
//           // log.warn("--After Response--");
            //  //System.out.println("Errrr : " + errors);

            response.sendRedirect(request.getContextPath() + url);
//           // log.warn("Redirect to : " + url);
//           // log.warn("Gen Errors : " + request.getAttribute("genResponse"));

            // //System.out.println("After Redirect");
        } catch (Exception e) {
            // //System.out.println("Error Occured : " + e.toString());
            e.printStackTrace();
            return;
        }
    }

    public static void successRedirect(String url, HttpServletRequest request, HttpServletResponse response, Map<String, String> errors) {
        try {
//           // log.warn("do RRdirect ");
            request.getSession().setAttribute("error", errors);
            request.getSession().setAttribute("response", "000");
//           // log.warn("--After Response--");
            //  //System.out.println("Errrr : " + errors);
            //request.setAttribute("PAGE_ID", getActionModel().actionByUrl(url));
            response.sendRedirect(request.getContextPath() + url);
//           // log.warn("Redirect to : " + url);
            // //System.out.println("After Redirect");
        } catch (Exception e) {
            // //System.out.println("Error Occured : " + e.toString());
            e.printStackTrace();
            return;
        }
    }

    public static void redirectWithGeneralInfo(String url, HttpServletRequest request, HttpServletResponse response, Map<String, String> errors) {
        try {
            // log.warn("do RRdirect ");
            //request.getSession().setAttribute("error", errors);
            request.getSession().setAttribute("response", "000");
            request.getSession().setAttribute("genResponse", errors);
            // log.warn("--After Response--");
            //  //System.out.println("Errrr : " + errors);
            //request.setAttribute("PAGE_ID", getActionModel().actionByUrl(url));
            response.sendRedirect(request.getContextPath() + url);
            // log.warn("Redirect to : " + url);
            // //System.out.println("After Redirect");
        } catch (Exception e) {
            // //System.out.println("Error Occured : " + e.toString());
            e.printStackTrace();
            return;
        }
    }

    public static String getApplicationLocationWithPort(HttpServletRequest request) {
        return request.getScheme() + "://" + request.getServerName() + ":" + request.getServerPort();
    }
    
     public static void redirect(HttpServletRequest request, HttpServletResponse response, String url) throws IOException {
		  	 
        try {
            String path = "";
            if (request.getHeader("referer") != null) {
                path = request.getHeader("referer");
            } else if (url != null || url == "") {
                path = url;
            } else {
                path = request.getContextPath();
            }
            
            request.getRequestDispatcher(url).forward(request, response);
            return;
        } catch (Exception e) {
            //System.out.println("Error Occured : " + e.toString());
            e.printStackTrace();
            return;
        }
    }

    public static String formatDecimal(double amt, int decimals) {
        return String.format("%." + decimals + "f", amt);
    }

    public static double formatToDecimal(double amt, int decimals) {
        return Double.parseDouble(formatDecimal(amt, decimals));
    }
    
    

}
