/**************************************************************************
Copyright (c) 2011:
Istituto Nazionale di Fisica Nucleare (INFN), Italy
Consorzio COMETA (COMETA), Italy

See http://www.infn.it and and http://www.consorzio-cometa.it for details on
the copyright holders.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

@author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
****************************************************************************/
package it.infn.ct;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

//import java.util.logging.Logger;
import org.apache.log4j.Logger;

/**
 * APIServerDaemonServlet Servlet used only to report APIServerDaemon execution
 * statistics and generic information
 * 
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 */
@WebServlet(name = "APIServerDaemonServlet", urlPatterns = { "/configuration" })
public class APIServerDaemonServlet extends HttpServlet {

    /*
     * Logger
     */
    private static final Logger _log = Logger.getLogger(APIServerDaemonServlet.class.getName());
    private static final String LS = System.getProperty("line.separator");

    // Configurations from properties file
    APIServerDaemonConfig asdConfig;

    // <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on
    // the + sign on the left to edit the code.">

    /**
     * Handles the HTTP <code>GET</code> method.
     *
     * @param request
     *            servlet request
     * @param response
     *            servlet response
     * @throws ServletException
     *             if a servlet-specific error occurs
     * @throws IOException
     *             if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
	    throws ServletException, IOException {
	processRequest(request, response);
	_log.info("GET: " + request);
    }

    /**
     * Handles the HTTP <code>POST</code> method.
     *
     * @param request
     *            servlet request
     * @param response
     *            servlet response
     * @throws ServletException
     *             if a servlet-specific error occurs
     * @throws IOException
     *             if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
	    throws ServletException, IOException {
	processRequest(request, response);
	_log.info("POST: " + request);
    }

    /**
     * Servlet init function
     * 
     * @param config
     * @throws javax.servlet.ServletException
     */
    @Override
    public void init(ServletConfig config) throws ServletException {

	// Read init parameters (web.xml)
	// String initParamValue = config.getInitParameter("initParamName");
	_log.debug("Loading preferences for Servlet");
	asdConfig = new APIServerDaemonConfig(false);
    }

    /**
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code>
     * methods.
     *
     * @param request
     *            servlet request
     * @param response
     *            servlet response
     * @throws ServletException
     *             if a servlet-specific error occurs
     * @throws IOException
     *             if an I/O error occurs
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
	    throws ServletException, IOException {
	response.setContentType("text/html;charset=UTF-8");

	/*
	 * try (PrintWriter out = response.getWriter()) { // TODO output your
	 * page here. You may use following sample code. out.println(
	 * "<!DOCTYPE html>"); out.println("<html>"); out.println("<head>");
	 * out.println("<title>Servlet APIServerDaemonServlet</title>");
	 * out.println("</head>"); out.println("<body>"); out.println(
	 * "<h1>Servlet APIServerDaemonServlet at " + request.getContextPath() +
	 * "</h1>"); out.println("<h2>APIServer configuration</h2>");
	 * out.println("<p>"+asdConfig.toString()+"</p>");
	 * out.println("</body>"); out.println("</html>"); }
	 */

	// Context path
	request.setAttribute("contextPath", request.getContextPath()); // This
								       // will
								       // be
								       // available
								       // as
								       // ${message}

	// APIServerDaemon DB settings
	request.setAttribute("apisrv_dbhost", asdConfig.getApisrv_dbhost());
	request.setAttribute("apisrv_dbport", asdConfig.getApisrv_dbport());
	request.setAttribute("apisrv_dbname", asdConfig.getApisrv_dbname());
	request.setAttribute("apisrv_dbuser", asdConfig.getApisrv_dbuser());
	request.setAttribute("apisrv_dbpass", asdConfig.getApisrv_dbpass());

	// APIServerDaemon Threads settings
	request.setAttribute("asdMaxThreads", asdConfig.getMaxThreads());
	request.setAttribute("asdCloseTimeout", asdConfig.getCloseTimeout());

	// GridEngine DB Settings
	request.setAttribute("utdb_jndi", asdConfig.getGridEngine_db_jndi());
	request.setAttribute("utdb_host", asdConfig.getGridEngine_db_host());
	request.setAttribute("utdb_port", asdConfig.getGridEngine_db_port());
	request.setAttribute("utdb_name", asdConfig.getGridEngine_db_name());
	request.setAttribute("utdb_user", asdConfig.getGridEngine_db_user());
	request.setAttribute("utdb_pass", asdConfig.getGridEngine_db_pass());

	// Render the HTML
	request.getRequestDispatcher("config.jsp").forward(request, response);
    }

    /**
     * Retrieves statistical information about APIServerDaemon activity in
     * particular it provides: - Start timestamp - Current timestamp - Number of
     * elements in the queue - Number of elements in the queue for each state
     * 
     * @param asdConnectionURL
     */
    void getAPIServerDaemonStatInfo(String asdConnectionURL) {
	APIServerDaemonDB asdDB = null;

	try {
	    _log.debug("Opening connection for retry command");
	    asdDB = new APIServerDaemonDB(asdConnectionURL);
	} catch (Exception e) {
	    _log.fatal("Unable retry task related to given command:" + LS + this.toString());
	} finally {
	    // if (asdDB != null) {
	    // asdDB.close();
	    // }
	    // _log.debug("Closing connection for update command");
	}
    }

    /**
     * Returns a short description of the servlet.
     *
     * @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
	return "Servlet used only to report GridEngineDaemon " + "execution statistics and generic information";
    } // </editor-fold>
}
