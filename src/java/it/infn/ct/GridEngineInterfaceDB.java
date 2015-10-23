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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;


/**
 * This class interfaces the GridEngine userstracking database; it helps
 * the GridEngineInterface class
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 * @see GridEngineInterface
 */
public class GridEngineInterfaceDB {
    /*
     GridEngine UsersTracking DB
    */    
    private String utdb_host;
    private String utdb_port;
    private String utdb_user;
    private String utdb_pass;
    private String utdb_name;
    private String connectionURL = null;
    
    /* 
      DB variables
    */    
    private Connection        connect           = null;
    private Statement         statement         = null;
    private PreparedStatement preparedStatement = null;
    private ResultSet         resultSet         = null;    
    
    /*
      Logger
    */
    private static final Logger _log = Logger.getLogger(GridEngineDaemonLogger.class.getName());
    
    public static final String LS = System.getProperty("line.separator");
    
    /**
     * Empty constructor for GridEngineInterface
     */
    public GridEngineInterfaceDB() {
        _log.info("Initializing GridEngineInterfaceDB");        
    }
    
    /**
     * Constructor that uses directly the JDBC connection URL
     * @param connectionURL jdbc connection URL containing: dbhost, dbport,
     * dbuser, dbpass and dbname in a single line
     */
    public GridEngineInterfaceDB(String connectionURL) { 
        this();
        this.connectionURL=connectionURL;
    }
    
    /**
     * Initializing GridEngineInterface using userstrackingdb
     * database connection settings
     * @param utdb_host UsersTrackingDB database hostname
     * @param utdb_port UsersTrackingDB database listening port
     * @param utdb_user UsersTrackingDB database user name
     * @param utdb_pass UsersTrackingDB database user password
     * @param utdb_name UsersTrackingDB database name
     */
    public GridEngineInterfaceDB(String utdb_host
                                ,String utdb_port
                                ,String utdb_user
                                ,String utdb_pass
                                ,String utdb_name) {
        this();        
        this.utdb_host=utdb_host;
        this.utdb_port=utdb_port;
        this.utdb_user=utdb_user;
        this.utdb_pass=utdb_pass;
        this.utdb_name=utdb_name;
        prepareConnectionURL();
    }
    
    /**
     * Prepare a connectionURL from detailed conneciton settings
     */
    private void prepareConnectionURL() {
        this.connectionURL="jdbc:mysql://" + utdb_host
                          +":"             + utdb_port
                          +"/"             + utdb_name
                          +"?user="        + utdb_user
                          +"&password="    + utdb_pass;
        _log.info("DBURL: '"+this.connectionURL+"'");
    }
    
    /**
     * Return object' connection URL
     * @return  GridEngineDaemon database connection URL
     */
    public String getConnectionURL() {
        return this.connectionURL;
    }
    
    /**
     * Connect to the GridEngineDaemon database
     * @return connect object
     */
    private boolean connect() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connect = DriverManager.getConnection(this.connectionURL);
        } catch (Exception e) {
            _log.severe("Unable to connect DB: '"+this.connectionURL+"'");
            _log.severe(e.toString());
        }
        _log.info("Connected to DB: '"+this.connectionURL+"'");
        return (connect != null);
    }
    
    /**
     * Close all db opened elements: resultset,statement,cursor,connection
    */
    public void close() {
        try {
            if(resultSet         != null) { resultSet.close();         resultSet         = null; }
            if(statement         != null) { statement.close();         statement         = null; }
            if(preparedStatement != null) { preparedStatement.close(); preparedStatement = null; }
            if(connect           != null) { connect.close();           connect           = null; }
        } catch (Exception e) {
            _log.severe("Unable to close DB: '"+this.connectionURL+"'");
            _log.severe(e.toString());
        }
        _log.info("Closed DB: '"+this.connectionURL+"'");
    }
    
    public String getJobStatus(int agi_id) {
        String jobStatus = null;
        if (!connect()) {
            _log.severe("Not connected to database");
            return jobStatus;
        }
        try {
            String sql;            
            sql="select status"               +LS
               +"from ActiveGridInteractions" +LS
               +"where id = ?;";               
            preparedStatement = connect.prepareStatement(sql);
            preparedStatement.setInt(1, agi_id);            
            resultSet=preparedStatement.executeQuery(); 
            resultSet.next();
            jobStatus=resultSet.getString("status");
        } catch (SQLException e) {            
            _log.severe(e.toString());
        }          
        return jobStatus;
    }

    /**
     * Get ActiveGridInteraction' id field from task_id
     * @param taskId
     * @return 
     */
    int getAGIId(int task_id) {
        int agi_id = 0;
        if (!connect()) {
            _log.severe("Not connected to database");
            return agi_id;
        }
        try {
            String jobDesc = "task_id: "+task_id;
            String sql;            
            sql="select id"                   +LS
               +"from ActiveGridInteractions" +LS
               +"where user_description = ?;";               
            preparedStatement = connect.prepareStatement(sql);
            preparedStatement.setString(1, jobDesc);            
            resultSet=preparedStatement.executeQuery(); 
            resultSet.next();
            agi_id = resultSet.getInt("id");
        } catch (SQLException e) {            
            _log.severe(e.toString());
        }        
        return agi_id;
    }
    
}
