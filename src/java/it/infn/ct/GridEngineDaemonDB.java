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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class managing any transaction on GridEngineDaemon database
 * This class takes care of retrieving commands from ge_queue table
 * and update its status records values accordingly to the command 
 * execution lifetime
 * Below the mapping of any possible (action,status) values in ge_queue table
 * ---------+------------------------------------------------------------
 * Action   | Statuses
 * ---------+------------------------------------------------------------
 * SUBMIT   | QUEUED|PROCESSING|PROCESSED
 *          | SUBMITTED|RUNNING|DONE|CANCELLED|ABORTED
 * GETSTATUS| This action never registers into ge_queue table
 *          | REST APIs directly returns the ge_queue state of the given task
 * GETOUTPUT| This action never registers into ge_queue table
 *          | REST APIs directly returns the ge_queue state of the given task
 * JOBCANCEL| QUEUED|PROCESSING|PROCESSED|CANCELLED
 * 
 * GridEngineDaemon foresees two different thread loops:
 *  - GridEngineDaemonPolling: This thread retrieves commands coming from
 *    the GridEngineDaemin API Server REST calls processing only tasks in
 *    QUEUED state and leaves them in PROCESSING state once processed
 *  - GridEngineDaeminController: This thread has the responsibility to verify
 *    the time consistency of any 'active' state in the queue and keep
 *    updated information about the real task state in acccordance with the
 *    GridEngine' ActiveGridInteraction (agi_id field)
 *    The controller thread is necessary because the any REST call related to
 *    job status or job output should ever return fresh information, thus
 *    the controller loop timely cross-check job status consistency between
 *    the ge_queue and the GridEngine' ActiveGridInteraction table
 * 
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 */
public class GridEngineDaemonDB {   
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
    private static final Logger _log = Logger.getLogger(GridEngineDaemonLogger.class.getName());
    
    public static final String LS = System.getProperty("line.separator");
    
    String threadName;
    
    /*
      Constructors ...
    */
    
    /**
     * Empty constructor, it do not fill DB connection settings
     */
    public GridEngineDaemonDB() {
        threadName = Thread.currentThread().getName();
    }    
    /**
     * Constructor that uses directly the JDBC connection URL
     * @param connectionURL jdbc connection URL containing: dbhost, dbport,
     * dbuser, dbpass and dbname in a single line
     */
    public GridEngineDaemonDB(String connectionURL) {
        threadName = Thread.currentThread().getName();
        this.connectionURL=connectionURL;
    }
    /**
     * Constructor that uses detailed connection settings used to buid the
     * JDBC connection URL
     * @param dbhost GridEngineDaemon database user name hostname
     * @param dbport GridEngineDaemon database user name listening port
     * @param dbuser GridEngineDaemon database user name
     * @param dbpass GridEngineDaemon database user password
     * @param dbname GridEngineDaemon database name
     */
    public GridEngineDaemonDB(String dbhost
                             ,String dbport
                             ,String dbuser
                             ,String dbpass
                             ,String dbname) {
        threadName = Thread.currentThread().getName();
        this.dbhost = dbhost;
        this.dbport = dbport;
        this.dbuser = dbuser;
        this.dbpass = dbpass;
        this.dbname = dbname;
        _log.info("DB: host='" + this.dbhost +
                   "', port='" + this.dbport +
                   "', user='" + this.dbuser +
                   "', pass='" + this.dbpass +
                   "', name='" + this.dbname
                 );
        prepareConnectionURL();        
    }        
    
    /**
     * Prepare a connectionURL from detailed conneciton settings
     */
    private void prepareConnectionURL() {
        this.connectionURL="jdbc:mysql://" + dbhost
                          +":"             + dbport
                          +"/"             + dbname
                          +"?user="        + dbuser
                          +"&password="    + dbpass;
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
    
    /**
     * Retrieves available commands for the GridEngine returning 
     * maxCommands records from the ge_queue table
     * Commands must be in QUEUED status while taken records will be
     * flagged as PROCESSING. 
     * Table ge_queue will be locked to avoid any inconsistency in
     * concurrent access
     * @param Maximum number of records to get from the ge_queue table
     * @retun A list of GridEngineDaemonCommand objects
     * @see GridEngineDaemonCommand
     */
    public List<GridEngineDaemonCommand> getQueuedCommands(int maxCommands) {        
        if (!connect()) {
            _log.severe("Not connected to database");
            return null;
        }
        List<GridEngineDaemonCommand> commandList = new ArrayList<>();
        try {
            String sql;
            // Lock ge_queue table first
            sql="lock tables ge_queue write;";
            statement=connect.createStatement();
            statement.execute(sql);
            sql="select task_id"                        +LS
               +"      ,agi_id"                         +LS
               +"      ,action"                         +LS
               +"      ,status"                         +LS
               +"      ,creation"                       +LS
               +"      ,last_change"                    +LS
               +"      ,action_info"                    +LS
               +"from ge_queue"                         +LS
               +"where status = 'QUEUED'"               +LS
               +"order by last_change asc"              +LS
               +"limit ?"                               +LS
               +";";
            preparedStatement = connect.prepareStatement(sql);
            preparedStatement.setInt(1, maxCommands);            
            resultSet=preparedStatement.executeQuery();
            while(resultSet.next()) {
                GridEngineDaemonCommand gedCmd = 
                        new GridEngineDaemonCommand(
                                resultSet.getInt   (    "task_id")
                               ,resultSet.getInt   (     "agi_id")
                               ,resultSet.getString(     "action")
                               ,resultSet.getString(     "status")
                               ,resultSet.getDate  (   "creation")                                       
                               ,resultSet.getDate  ("last_change")
                               ,resultSet.getString("action_info"));                
                if (null != gedCmd) commandList.add(gedCmd);
                _log.info("Loaded command: "+LS+gedCmd);                
            }
            // change status to the taken commands as PROCESSING
            Iterator<GridEngineDaemonCommand> iterCmds = commandList.iterator(); 
            while (iterCmds.hasNext()) {
                GridEngineDaemonCommand geCommand = iterCmds.next();                
                sql="update ge_queue set status = 'PROCESSING'"+LS
                   +"                   ,last_change = now()"  +LS
                   +"where task_id=?";
                preparedStatement = connect.prepareStatement(sql);
                preparedStatement.setInt(1, geCommand.getTaskId());            
                preparedStatement.execute();                
            }
            // Unlock ge_queue table
            sql="unlock tables;";
            statement=connect.createStatement();
            statement.execute(sql);
        } catch (SQLException e) {            
            _log.severe(e.toString());
        }        
        return commandList;
    }
    
    /**
     * Retrieves available commands for the GridEngine returning 
     * maxCommands records from the ge_queue table
     * Commands must be in QUEUED status while taken records will be
     * flagged as PROCESSING. 
     * Table ge_queue will be locked to avoid any inconsistency in
     * concurrent access
     * @param Maximum number of records to get from the ge_queue table
     * @retun A list of GridEngineDaemonCommand objects
     * @see GridEngineDaemonCommand
     */
    public List<GridEngineDaemonCommand> 
        getControllerCommands(int maxCommands) {        
        if (!connect()) {
            _log.severe("Not connected to database");
            return null;
        }
        List<GridEngineDaemonCommand> commandList = new ArrayList<>();
        try {
            String sql;
            // Lock ge_queue table first
            sql="select task_id"                        +LS
               +"      ,agi_id"                         +LS
               +"      ,action"                         +LS
               +"      ,status"                         +LS
               +"      ,creation"                       +LS
               +"      ,last_change"                    +LS
               +"      ,action_info"                    +LS
               +"from ge_queue"                         +LS
               +"where status in ('PROCESSING'"         +LS
               + "               ,'SUBMITTED'"          +LS
               + "               ,'RUNNING')"           +LS
               +"order by last_change asc"              +LS
               +"limit ?"                               +LS
               +";";
            preparedStatement = connect.prepareStatement(sql);
            preparedStatement.setInt(1, maxCommands);            
            resultSet=preparedStatement.executeQuery();
            while(resultSet.next()) {
                GridEngineDaemonCommand gedCmd = 
                        new GridEngineDaemonCommand(
                                resultSet.getInt   (    "task_id")
                               ,resultSet.getInt   (     "agi_id")
                               ,resultSet.getString(     "action")
                               ,resultSet.getString(     "status")
                               ,resultSet.getDate  (   "creation")                                       
                               ,resultSet.getDate  ("last_change")
                               ,resultSet.getString("action_info"));                
                if (null != gedCmd) commandList.add(gedCmd);
                _log.info("Loaded command: "+LS+gedCmd);                
            }
        } catch (SQLException e) {            
            _log.severe(e.toString());
        }        
        return commandList;
    }
    
    /**
     * Release a given command setting its status to PROCESSED 
     * @param GridEngineCommand object
     * @see GridEngineCommand
    */
    public void releaseCommand(GridEngineDaemonCommand command,String status) {
        if(status == null)
            setCommandState(command,"PROCESSED");
        else setCommandState(command,status);
    }
    
    /**
     * Assign to the given command a given status 
     * @param GridEngineCommand object
     * @param New command status
     * @see GridEngineCommand
    */    
    private void setCommandState(GridEngineDaemonCommand command
                                ,String status) {    
        if (!connect()) {
            _log.severe("Not connected to database");
            return;
        }        
        try {
            String sql;
            // Lock ge_queue table first
            sql="lock tables ge_queue write;";
            statement=connect.createStatement();
            statement.execute(sql);
            sql="update ge_queue set status = ?"         +LS
                   +"               ,agi_id = ?"         +LS
                   +"               ,last_update = now()"+LS
                   +"where task_id=?";
            preparedStatement = connect.prepareStatement(sql);
            preparedStatement.setString(1, status);
            preparedStatement.setInt(2, command.getAGIId());
            preparedStatement.setInt(3, command.getTaskId());
            preparedStatement.execute();                               
            // Unlock ge_queue table
            sql="unlock tables;";
            statement=connect.createStatement();
            statement.execute(sql);
            _log.severe("released command: "+LS+command);
        } catch (SQLException e) {            
            _log.severe(e.toString());
        }
    }
    
}
