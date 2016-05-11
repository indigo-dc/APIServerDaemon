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
//import java.util.logging.Logger;
import org.apache.log4j.Logger;

/**
 * This class interfaces the SimpleTosca database table; 
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 * @see SimpleToscaInterface
 */
public class SimpleToscaInterfaceDB {
    /*
     GridEngine UsersTracking DB
    */    
    private String asdb_host;
    private String asdb_port;
    private String asdb_user;
    private String asdb_pass;
    private String asdb_name;
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
    private static final Logger _log = Logger.getLogger(SimpleToscaInterfaceDB.class.getName());
    
    public static final String LS = System.getProperty("line.separator");
    
    /**
     * Empty constructor for SimpleToscaInterface
     */
    public SimpleToscaInterfaceDB() {
        _log.debug("Initializing SimpleToscaInterfaceDB");        
    }
    
    /**
     * Constructor that uses directly the JDBC connection URL
     * @param connectionURL jdbc connection URL containing: dbhost, dbport,
     * dbuser, dbpass and dbname in a single line
     */
    public SimpleToscaInterfaceDB(String connectionURL) { 
        this();
        _log.debug("SimpleTosca connection URL:"+LS+connectionURL);
        this.connectionURL=connectionURL;
    }
    
    /**
     * Initializing SimpleToscaInterface database 
     * database connection settings
     * @param asdb_host APIServerDaemon database hostname
     * @param asdb_port APIServerDaemon database listening port
     * @param asdb_user APIServerDaemon database user name
     * @param asdb_pass APIServerDaemon database user password
     * @param asdb_name APIServerDaemon database name
     */
    public SimpleToscaInterfaceDB(String asdb_host
                                 ,String asdb_port
                                 ,String asdb_user
                                 ,String asdb_pass
                                 ,String asdb_name) {
        this();        
        this.asdb_host=asdb_host;
        this.asdb_port=asdb_port;
        this.asdb_user=asdb_user;
        this.asdb_pass=asdb_pass;
        this.asdb_name=asdb_name;
        prepareConnectionURL();
    }
    
    /**
     * Prepare a connectionURL from detailed conneciton settings
     */
    private void prepareConnectionURL() {
        this.connectionURL="jdbc:mysql://" + asdb_host
                          +":"             + asdb_port
                          +"/"             + asdb_name
                          +"?user="        + asdb_user
                          +"&password="    + asdb_pass;
        _log.debug("SimpleToscaInterface connectionURL: '"+this.connectionURL+"'");
    }
    
    /**
     * Return object' connection URL
     * @return SimpleToscaInterface database connection URL
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
            _log.fatal("Unable to connect DB: '"+this.connectionURL+"'");          
            _log.fatal(e.toString());
        }
        _log.debug("Connected to DB: '"+this.connectionURL+"'");
        return (connect != null);
    }
    
    /**
     * Close all db opened elements except the connection
     */
    public void closeSQLActivity() 
    {
        try {
            if(resultSet         != null) { _log.debug("closing resultSet");         resultSet.close();         resultSet         = null; }
            if(statement         != null) { _log.debug("closing statement");         statement.close();         statement         = null; }
            if(preparedStatement != null) { _log.debug("closing preparedStatement"); preparedStatement.close(); preparedStatement = null; }        
        } catch(SQLException e) {
            _log.fatal("Unable to close SQLActivities (resultSet, statement, preparedStatement)");          
            _log.fatal(e.toString());
        }
    }
    
    /**
     * Close all db opened elements: resultset,statement,cursor,connection
    */
    public void close() {
        closeSQLActivity();
        try {
            if(connect           != null) { connect.close();           connect           = null; }
        } catch (Exception e) {          
            _log.fatal("Unable to close DB: '"+this.connectionURL+"'");          
            _log.fatal(e.toString());
        }
        _log.info("Closed DB: '"+this.connectionURL+"'");
    }  

    /**
     * Get toscaId
     * @param toscaCommand
     * @return toscaid
     */ 
    public String  getToscaId(APIServerDaemonCommand toscaCommand) {
        String toscaId = "";
        if (!connect()) {          
            _log.fatal("Not connected to database");
            return toscaId;
        }
        try {
            String sql;            
            sql="select tosca_id"   +LS
               +"from simple_tosca" +LS
               +"where task_id = ?;";               
            preparedStatement = connect.prepareStatement(sql);
            preparedStatement.setInt(1, toscaCommand.getTaskId());  
            resultSet=preparedStatement.executeQuery(); 
            if(resultSet.next())
                toscaId = resultSet.getString("tosca_id");
        } catch (SQLException e) {                      
            _log.fatal(e.toString());
        } finally {
            closeSQLActivity();
        }       
        return toscaId;
    }  

    
    //
    // Register the tosca_id of the given toscaCommand
    // @param toscaCommand
    // @oaram toscaId
    // 
    public int registerToscaId(APIServerDaemonCommand toscaCommand, String toscaId) {
        int tosca_id = 0;
        if (!connect()) {          
            _log.fatal("Not connected to database"); 
            return tosca_id;
        }
        try {
            String sql;
            // Lock ge_queue table first
            sql="lock tables simple_tosca write, simple_tosca as st read;";
            statement=connect.createStatement();
            statement.execute(sql);
            // Insert new entry for simple tosca
            sql="insert into simple_tosca (id,task_id, tosca_id, tosca_status, creation, last_change)" + LS 
               +"select (select if(max(id)>0,max(id)+1,1) from simple_tosca st),?,?,'SUBMITTED',now(),now();";
            preparedStatement = connect.prepareStatement(sql);
            preparedStatement.setInt   (1, toscaCommand.getTaskId());
            preparedStatement.setString(2, toscaId); 
            preparedStatement.execute();       
            // Get the new Id
            sql="select id from simple_tosca where tosca_id = ?;";
            preparedStatement = connect.prepareStatement(sql);
            preparedStatement.setString(1, toscaId);
            resultSet=preparedStatement.executeQuery();
            if(resultSet.next())
                tosca_id = resultSet.getInt("id"); 
            // Unlock tables
            sql="unlock tables;";
            statement.execute(sql);
        } catch (SQLException e) {                      
            _log.fatal(e.toString());
        } finally {
            closeSQLActivity();
        }
        return tosca_id;  
    }

    /**
     * Update the toscaId value into an existing simple_tosca record
     * @param simpleToscaId record index in simple_tosca table
     * @oaram toscaUUID tosca submission UUID field
     */
    public void updateToscaId(int simpleToscaId, String toscaUUID) {
        if (!connect()) {
            _log.fatal("Not connected to database");
            return;
        }
        try {
            String sql;
            // Lock ge_queue table first
            sql="lock tables simple_tosca write;";
            statement=connect.createStatement();
            statement.execute(sql);
            // Insert new entry for simple tosca
            sql="update simple_tosca set tosca_id=?, tosca_status='SUBMITTED', creation=now(), last_change=now() where id=?;";
            preparedStatement = connect.prepareStatement(sql);
            preparedStatement.setString(1, toscaUUID);
            preparedStatement.setInt   (2, simpleToscaId);                                                                                    
            preparedStatement.execute();
            sql="unlock tables;";
            statement.execute(sql);
        } catch (SQLException e) {
            _log.fatal(e.toString());
        } finally {
            closeSQLActivity();
        }
    }
        
    
    /**
     * Update the tosca status value into an existing simple_tosca record
     * @param simpleToscaId record index in simple_tosca table
     * @oaram toscaStatus tosca submission status
     */
    public void updateToscaStatus(int simpleToscaId, String toscaStatus) {
        if (!connect()) {
            _log.fatal("Not connected to database");
            return;
        }
        try {
            String sql;
            // Lock ge_queue table first
            sql="lock tables simple_tosca write;";
            statement=connect.createStatement();
            statement.execute(sql);
            // Insert new entry for simple tosca
            sql="update simple_tosca set tosca_status=?, last_change=now() where id=?;";
            preparedStatement = connect.prepareStatement(sql);
            preparedStatement.setString(1, toscaStatus);
            preparedStatement.setInt   (2, simpleToscaId);            
            preparedStatement.execute();
            sql="unlock tables;";
            statement.execute(sql);
        } catch (SQLException e) {
            _log.fatal(e.toString());
        } finally {
            closeSQLActivity();
        }
    }
}
