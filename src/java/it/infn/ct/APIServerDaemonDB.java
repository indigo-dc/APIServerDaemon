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
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
//import java.util.logging.Logger;
import org.apache.log4j.Logger;

/**
 * Class managing any transaction on APIServerDaemon database
 * This class takes care of retrieving commands from ge_queue table
 * and update its status records values accordingly to the command 
 * execution lifetime
 * Below the mapping of any possible (action,status) values in ge_queue table
 * ---------+------------------------------------------------------------
 * Action   | Statuses
 * ---------+------------------------------------------------------------
 * SUBMIT   | QUEUED|PROCESSING|PROCESSED|FAILED|DONE
 * GETSTATUS| This action never registers into ge_queue table
 *          | REST APIs directly returns the ge_queue state of the given task
 * GETOUTPUT| This action never registers into ge_queue table
 *          | REST APIs directly returns the ge_queue state of the given task
 * JOBCANCEL| QUEUED|PROCESSING|PROCESSED|FAILED|CANCELLED
 * 
 * APIServerDaemon foresees two different thread loops:
 *  - APIServerDaemonPolling: This thread retrieves commands coming from
 *    the APIServerDaemon API Server REST calls processing only tasks in
 *    QUEUED state and leaves them in PROCESSING state once processed
 *  - APIServerDaeminController: This thread has the responsibility to verify
 *    the time consistency of any 'active' state in the queue and keep
 *    updated information about the real task state in accordance inside the
 *    underlying target architecture; for instance the GridEngine' 
 *     ActiveGridInteraction (agi_id field) to check job status
 *    The controller thread is necessary because the any REST call related to
 *    job status or job output should ever return fresh information, thus
 *    the controller loop timely cross-check job status consistency between
 *    the as_queue and the targeted architecture; for instance the
 *    GridEngine' ActiveGridInteraction table. Each specific architecture 
 *    change is in charge of the specific interface object
 * 
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 */
public class APIServerDaemonDB {   
    /*
      DB connection settings
    */
    private String dbname = "";
    private String dbhost = "";
    private String dbport = "";
    private String dbuser = "";
    private String dbpass = "";
    private String connectionURL = null;
    /* 
      DB variables
    */    
    private Connection        connect           = null;
    private Statement         statement         = null;
    private PreparedStatement preparedStatement = null;
    private ResultSet         resultSet         = null;
    
    /**
     * Logger
     */
    private static final Logger _log = Logger.getLogger(APIServerDaemonDB.class.getName());
    
    public static final String LS = System.getProperty("line.separator");
    
    String threadName;
    
    private static String driverURL = "com.mysql.jdbc.Driver";
    
    /**
     * Register the MySQL driver
     */
    public static void registerDriver() {
        try {        
            _log.debug("Registering driver: '"+driverURL+"'");
            Class.forName(driverURL);
        } catch (ClassNotFoundException e) {
            _log.fatal("Unable to unregister driver: '"+driverURL+"'");
        }
    }
    
    /**
     * Unregister MySQL driver
     */
    public static void unregisterDriver() {        
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            try {
                DriverManager.deregisterDriver(driver);
                _log.info(String.format("deregistering jdbc driver: %s", driver));
            } catch (SQLException e) {
                _log.fatal(String.format("Error deregistering driver %s", driver), e);
            }
        }
    }
    
    /*
      Constructors ...
    */
    
    /**
     * Empty constructor, it do not fill DB connection settings
     */
    public APIServerDaemonDB() {
        threadName = Thread.currentThread().getName();
    }    
    /**
     * Constructor that uses directly the JDBC connection URL
     * @param connectionURL jdbc connection URL containing: dbhost, dbport,
     * dbuser, dbpass and dbname in a single line
     */
    public APIServerDaemonDB(String connectionURL) {
        this();
        this.connectionURL=connectionURL;
    }
    /**
     * Constructor that uses detailed connection settings used to buid the
     * JDBC connection URL
     * @param dbhost APIServerDaemon database hostname
     * @param dbport APIServerDaemon database listening port
     * @param dbuser APIServerDaemon database user name
     * @param dbpass APIServerDaemon database user password
     * @param dbname APIServerDaemon database name
     */
    public APIServerDaemonDB( String dbhost
                             ,String dbport
                             ,String dbuser
                             ,String dbpass
                             ,String dbname) {
        this();
        this.dbhost = dbhost;
        this.dbport = dbport;
        this.dbuser = dbuser;
        this.dbpass = dbpass;
        this.dbname = dbname;
        _log.debug(
                   "DB:host='" + this.dbhost +
                   "', port='" + this.dbport +
                   "', user='" + this.dbuser +
                   "', pass='" + this.dbpass +
                   "', name='" + this.dbname
                 );
        prepareConnectionURL();        
    }        
    
    /**
     * Prepare a connectionURL from detailed connection settings
     */
    private void prepareConnectionURL() {
        this.connectionURL="jdbc:mysql://" + dbhost
                          +":"             + dbport
                          +"/"             + dbname
                          +"?user="        + dbuser
                          +"&password="    + dbpass;
        _log.debug("DBURL: '"+this.connectionURL+"'");
    }
    
    /**
     * Return object' connection URL
     * @return  APIServerDaemon database connection URL
     */
    public String getConnectionURL() {
        return this.connectionURL;
    }
    
    /**
     * Connect to the APIServerDaemon database
     * @return connect object
     */
    private boolean connect() {
        try {
          //Class.forName("com.mysql.jdbc.Driver");
            connect = DriverManager.getConnection(this.connectionURL);
        } catch (Exception e) {          
            _log.fatal("Unable to connect DB: '"+this.connectionURL+"'");          
            _log.fatal(e.toString());
        }
        _log.debug("Connected to DB: '"+this.connectionURL+"'");
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
            _log.fatal("Unable to close DB: '"+this.connectionURL+"'");          
            _log.fatal(e.toString());
        }
        _log.debug("Closed DB: '"+this.connectionURL+"'");
    } 
    
    /**
     * Retrieves available commands for the APIServer returning 
     * maxCommands records from the as_queue table
     * Commands must be in QUEUED status while taken records will be
     * flagged as PROCESSING. 
     * Table as_queue will be locked to avoid any inconsistency in
     * concurrent access
     * @param Maximum number of records to get from the as_queue table
     * @retun A list of APIServerDaemonCommand objects
     * @see APIServerDaemonCommand
     */
    public List<APIServerDaemonCommand> getQueuedCommands(int maxCommands) {        
        if (!connect()) {          
            _log.fatal("Not connected to database");
            return null;
        }
        List<APIServerDaemonCommand> commandList = new ArrayList<>();
        try {
            String sql;
            // Lock ge_queue table first
            sql="lock tables as_queue write;";
            statement=connect.createStatement();
            statement.execute(sql);
            sql="select task_id"                        +LS               
               +"      ,target_id"                      +LS
               +"      ,target"                         +LS
               +"      ,action"                         +LS
               +"      ,status"                         +LS
               +"      ,ge_status"                      +LS     
               +"      ,creation"                       +LS
               +"      ,last_change"                    +LS
               +"      ,action_info"                    +LS
               +"from as_queue"                         +LS
               +"where status = 'QUEUED'"               +LS
               +"order by last_change asc"              +LS
               +"limit ?"                               +LS
               +";";
            preparedStatement = connect.prepareStatement(sql);
            preparedStatement.setInt(1, maxCommands);            
            resultSet=preparedStatement.executeQuery();
            while(resultSet.next()) {
                APIServerDaemonCommand asdCmd = 
                        new APIServerDaemonCommand(
                                resultSet.getInt   (    "task_id")                               
                               ,resultSet.getInt   (  "target_id")
                               ,resultSet.getString(     "target") 
                               ,resultSet.getString(     "action")
                               ,resultSet.getString(     "status")
                               ,resultSet.getString(  "ge_status")
                               ,resultSet.getDate  (   "creation")                                       
                               ,resultSet.getDate  ("last_change")
                               ,resultSet.getString("action_info"));                
                if (null != asdCmd) commandList.add(asdCmd);
                _log.debug("Loaded command: "+LS+asdCmd);                
            }
            // change status to the taken commands as PROCESSING
            Iterator<APIServerDaemonCommand> iterCmds = commandList.iterator(); 
            while (iterCmds.hasNext()) {
                APIServerDaemonCommand asCommand = iterCmds.next();                
                sql="update as_queue set status = 'PROCESSING'"+LS
                   +"                   ,last_change = now()"  +LS
                   +"where task_id=?";
                preparedStatement = connect.prepareStatement(sql);
                preparedStatement.setInt(1, asCommand.getTaskId());            
                preparedStatement.execute();                
            }
            // Unlock ge_queue table
            sql="unlock tables;";
            statement=connect.createStatement();
            statement.execute(sql);
        } catch (SQLException e) {                      
            _log.fatal(e.toString());
        }        
        return commandList;
    }
    
    /**
     * Retrieves available commands for the APIServer returning 
     * maxCommands records from the as_queue table
     * Commands must be in QUEUED status while taken records will be
     * flagged as PROCESSING
     * Table as_queue will be locked to avoid any inconsistency in
     * concurrent access
     * @param Maximum number of records to get from the ge_queue table
     * @retun A list of APIServerDaemonCommand objects
     * @see APIServerDaemonCommand
     */
    public List<APIServerDaemonCommand> 
        getControllerCommands(int maxCommands) {        
        if (!connect()) {          
            _log.fatal("Not connected to database");
            return null;
        }
        List<APIServerDaemonCommand> commandList = new ArrayList<>();
        try {
            String sql;
            // Lock ge_queue table first
            sql="select task_id"                        +LS
               +"      ,target_id"                      +LS
               +"      ,target"                         +LS
               +"      ,action"                         +LS
               +"      ,status"                         +LS
               +"      ,ge_status"                      +LS     
               +"      ,creation"                       +LS
               +"      ,last_change"                    +LS
               +"      ,action_info"                    +LS
               +"from as_queue"                         +LS
               +"where status = 'PROCESSING'"           +LS
               + "  or status = 'PROCESSED'"            +LS               
               +"order by last_change asc"              +LS
               +"limit ?"                               +LS
               +";";
            preparedStatement = connect.prepareStatement(sql);
            preparedStatement.setInt(1, maxCommands);            
            resultSet=preparedStatement.executeQuery();
            while(resultSet.next()) {
                APIServerDaemonCommand asdCmd = 
                        new APIServerDaemonCommand(
                                resultSet.getInt   (    "task_id")
                               ,resultSet.getInt   (  "target_id")
                               ,resultSet.getString(     "target") 
                               ,resultSet.getString(     "action")
                               ,resultSet.getString(     "status")
                               ,resultSet.getString(  "ge_status")
                               ,resultSet.getDate  (   "creation")                                       
                               ,resultSet.getDate  ("last_change")
                               ,resultSet.getString("action_info"));                
                if (null != asdCmd) commandList.add(asdCmd);
                _log.debug("Loaded command: "+LS+asdCmd);                
            }
        } catch (SQLException e) {                      
            _log.fatal(e.toString());
        }        
        return commandList;
    }       
    
    /**
     * Update values of a given command except for: date fields and 
     * action_info; creation date will be ignored, while last_change 
     * will be set to now()
     * @param APIServerCommand object
     * @param New command status
     * @see APIServerCommand
    */    
    public void updateCommand(APIServerDaemonCommand command) {    
        if (!connect()) {          
            _log.fatal("Not connected to database");
            return;
        }        
        try {
            String sql;
            // Lock ge_queue table first
            sql="lock tables as_queue write;";
            statement=connect.createStatement();
            statement.execute(sql);
            sql="update as_queue set target_id = ?"      +LS
               +"                   ,status = ?"         +LS  
               +"                   ,ge_status = ?"      +LS
               +"                   ,last_change = now()"+LS
               +"where task_id=?"                        +LS
               +"  and action=?";
            preparedStatement = connect.prepareStatement(sql);
            preparedStatement.setInt   (1, command.getTargetId());            
            preparedStatement.setString(2, command.getStatus());
            preparedStatement.setString(3, command.getTargetStatus());
            preparedStatement.setInt   (4, command.getTaskId());
            preparedStatement.setString(5, command.getAction());
            preparedStatement.execute();                               
            // Unlock ge_queue table
            sql="unlock tables;";
            statement=connect.createStatement();
            statement.execute(sql);
            _log.debug("Updated command in as_queue: "+LS+command);            
            // propagate Status change in APIServer' task table
            updateAPIServerStatus(command);
        } catch (SQLException e) {                      
            _log.fatal(e.toString());
        }
    }
    
    /**
     * Update APIServer command status setting the GE status in task table
     */
    void updateAPIServerStatus(APIServerDaemonCommand command) {
        if (!connect()) {          
            _log.fatal("Not connected to database");
            return;
        }        
        try {
            String sql;
            // Lock ge_queue table first
            sql="lock tables task write;";
            statement=connect.createStatement();
            statement.execute(sql);
            sql="update task set status = ?"         +LS  
               +"               ,last_change = now()"+LS
               +"where id=?";
            String newStatus=(command.getTargetStatus()==null?
                              "WAITING":command.getTargetStatus());
            preparedStatement = connect.prepareStatement(sql);
            preparedStatement.setString(1, newStatus);
            preparedStatement.setInt   (2, command.getTaskId());
            preparedStatement.execute();                               
            // Unlock ge_queue table
            sql="unlock tables;";
            statement=connect.createStatement();
            statement.execute(sql);
            _log.debug("Updated status to: '"+newStatus
                     +"'for task: "+command.getTaskId());
        } catch (SQLException e) {                      
            _log.fatal(e.toString());
        }
    }
    
    /**
     * Update output paths of a given command
     */
    void updateOutputPaths(APIServerDaemonCommand command
                          ,String outputDir) {
        if (!connect()) {          
            _log.fatal("Not connected to database");
            return;
        }        
        try {
            String sql;
            // Lock ge_queue table first
            sql="lock tables task_output_file write;";
            statement=connect.createStatement();
            statement.execute(sql);
            sql="update task_output_file set path = ?"  +LS                 
               +"where task_id=?";
            preparedStatement = connect.prepareStatement(sql);
            preparedStatement.setString(1, command.getActionInfo()+"/"
                                          +outputDir);
            preparedStatement.setInt   (2, command.getTaskId());
            preparedStatement.execute();                               
            // Unlock ge_queue table
            sql="unlock tables;";
            statement=connect.createStatement();
            statement.execute(sql);
            _log.debug("Output dir '"+command.getActionInfo()+"/"+outputDir
                      +"' updated");
        } catch (SQLException e) {                      
            _log.fatal(e.toString());
        }
    }
    
    /**
     * Delete any task entry from the API Server DB including
     * the queue
     */
    void removeTaksEntries(int task_id) {
        if (!connect()) {          
            _log.fatal("Not connected to database");
            return;
        }        
        try {
            String sql;
            //
            // Table task_output_file
            //
            // Lock ge_queue table first
            sql="lock tables task_output_file write;";
            statement=connect.createStatement();
            statement.execute(sql);
            // Delete entries in task_output_file
            sql="delete from task_output_file where task_id = ?;";
            preparedStatement = connect.prepareStatement(sql);
            preparedStatement.setInt(1, task_id);
            preparedStatement.execute();                               
            // Unlock task_output_file table
            sql="unlock tables;";
            statement=connect.createStatement();
            statement.execute(sql);            
            //
            // Table task_input_file
            //
            // Lock task_input_file table first
            sql="lock tables task_input_file write;";
            statement=connect.createStatement();
            statement.execute(sql);
            // Delete entries in task_input_file
            sql="delete from task_input_file where task_id = ?;";
            preparedStatement = connect.prepareStatement(sql);
            preparedStatement.setInt(1, task_id);
            preparedStatement.execute();                               
            // Unlock task_input_file table
            sql="unlock tables;";
            statement=connect.createStatement();
            statement.execute(sql);            
            //
            // Table task_arguments
            //
            // Lock task_arguments table first
            sql="lock tables task_arguments write;";
            statement=connect.createStatement();
            statement.execute(sql);
            // Delete entries in ge_queue
            sql="delete from task_arguments where task_id = ?;";
            preparedStatement = connect.prepareStatement(sql);
            preparedStatement.setInt(1, task_id);
            preparedStatement.execute();                               
            // Unlock task_arguments table
            sql="unlock tables;";
            statement=connect.createStatement();
            statement.execute(sql);                        
            //
            // Table as_queue
            //
            // Lock ge_queue table first
            sql="lock tables as_queue write;";
            statement=connect.createStatement();
            statement.execute(sql);
            // Delete entries in ge_queue
            sql="delete from as_queue where task_id = ?;";
            preparedStatement = connect.prepareStatement(sql);
            preparedStatement.setInt(1, task_id);
            preparedStatement.execute();                               
            // Unlock ge_queue table
            sql="unlock tables;";
            statement=connect.createStatement();
            statement.execute(sql);                                                
            //
            // Table task
            //
            // Lock task table first
            sql="lock tables task write;";
            statement=connect.createStatement();
            statement.execute(sql);
            // Delete entries in task
            sql="delete from task where id = ?;";
            preparedStatement = connect.prepareStatement(sql);
            preparedStatement.setInt(1, task_id);
            preparedStatement.execute();                               
            // Unlock task table
            sql="unlock tables;";
            statement=connect.createStatement();
            statement.execute(sql);            
            _log.debug("All entries for task '"+task_id+"' have been removed");
        } catch (SQLException e) {                      
            _log.fatal(e.toString());
        }
    }
    
}
