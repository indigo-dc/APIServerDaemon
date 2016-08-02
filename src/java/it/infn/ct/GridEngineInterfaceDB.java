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
 * This class interfaces the GridEngine userstracking database; it helps
 * the GridEngineInterface class
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 * @see GridEngineInterface
 */
public class GridEngineInterfaceDB {

    /*
     * Logger
     */
    private static final Logger _log          = Logger.getLogger(GridEngineInterfaceDB.class.getName());
    public static final String  LS            = System.getProperty("line.separator");
    private String              connectionURL = null;

    /*
     * DB variables
     */
    private Connection        connect           = null;
    private Statement         statement         = null;
    private PreparedStatement preparedStatement = null;
    private ResultSet         resultSet         = null;

    /*
     * GridEngine UsersTracking DB
     */
    private String utdb_host;
    private String utdb_port;
    private String utdb_user;
    private String utdb_pass;
    private String utdb_name;

    /**
     * Empty constructor for GridEngineInterface
     */
    public GridEngineInterfaceDB() {
        _log.debug("Initializing GridEngineInterfaceDB");
    }

    /**
     * Constructor that uses directly the JDBC connection URL
     * @param connectionURL jdbc connection URL containing: dbhost, dbport,
     * dbuser, dbpass and dbname in a single line
     */
    public GridEngineInterfaceDB(String connectionURL) {
        this();
        _log.debug("GridEngineInterfaceDB connection URL:" + LS + connectionURL);
        this.connectionURL = connectionURL;
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
    public GridEngineInterfaceDB(String utdb_host, String utdb_port, String utdb_user, String utdb_pass,
                                 String utdb_name) {
        this();
        this.utdb_host = utdb_host;
        this.utdb_port = utdb_port;
        this.utdb_user = utdb_user;
        this.utdb_pass = utdb_pass;
        this.utdb_name = utdb_name;
        prepareConnectionURL();
    }

    /**
     * Close all db opened elements: resultset,statement,cursor,connection
     */
    public void close() {
        closeSQLActivity();

        try {
            if (connect != null) {
                connect.close();
                connect = null;
            }
        } catch (Exception e) {
            _log.fatal("Unable to close DB: '" + this.connectionURL + "'");
            _log.fatal(e.toString());
        }

        _log.info("Closed DB: '" + this.connectionURL + "'");
    }

    /**
     * Close all db opened elements except the connection
     */
    public void closeSQLActivity() {
        try {
            if (resultSet != null) {
                _log.debug("closing resultSet");
                resultSet.close();
                resultSet = null;
            }

            if (statement != null) {
                _log.debug("closing statement");
                statement.close();
                statement = null;
            }

            if (preparedStatement != null) {
                _log.debug("closing preparedStatement");
                preparedStatement.close();
                preparedStatement = null;
            }
        } catch (SQLException e) {
            _log.fatal("Unable to close SQLActivities (resultSet, statement, preparedStatement)");
            _log.fatal(e.toString());
        }
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
            _log.fatal("Unable to connect DB: '" + this.connectionURL + "'");
            _log.fatal(e.toString());
        }

        _log.debug("Connected to DB: '" + this.connectionURL + "'");

        return (connect != null);
    }

    /**
     * Prepare a connectionURL from detailed conneciton settings
     */
    private void prepareConnectionURL() {
        this.connectionURL = "jdbc:mysql://" + utdb_host + ":" + utdb_port + "/" + utdb_name + "?user=" + utdb_user
                             + "&password=" + utdb_pass;
        _log.debug("DBURL: '" + this.connectionURL + "'");
    }

    /**
     * Remove the given record form ActiveGridInteraction table
     * @param ActiveGridInteraction id
     */
    public void removeAGIRecord(int agi_id) {
        if (!connect()) {
            _log.fatal("Not connected to database");

            return;
        }

        try {
            String sql;

            sql               = "delete from ActiveGridInteractions" + LS + "where id = ?;";
            preparedStatement = connect.prepareStatement(sql);
            preparedStatement.setInt(1, agi_id);
            resultSet = preparedStatement.executeQuery();
        } catch (SQLException e) {
            _log.fatal(e.toString());
        } finally {
            closeSQLActivity();
        }
    }

    /**
     * Get ActiveGridInteraction' id field from task_id
     * @param taskId
     * @return
     */
    int getAGIId(APIServerDaemonCommand geCommand) {
        int agi_id = 0;

        if (!connect()) {
            _log.fatal("Not connected to database");

            return agi_id;
        }

        try {
            String jobDesc = geCommand.getTaskId() + "@" + geCommand.getActionInfo();
            String sql;

            sql               = "select id" + LS + "from ActiveGridInteractions" + LS + "where user_description = ?;";
            preparedStatement = connect.prepareStatement(sql);
            preparedStatement.setString(1, jobDesc);
            resultSet = preparedStatement.executeQuery();

            if (resultSet.next()) {
                agi_id = resultSet.getInt("id");
            }
        } catch (SQLException e) {
            _log.fatal(e.toString());
        } finally {
            closeSQLActivity();
        }

        return agi_id;
    }

    /**
     * Return object' connection URL
     * @return  GridEngineDaemon database connection URL
     */
    public String getConnectionURL() {
        return this.connectionURL;
    }

    /**
     * Get description of the given ActiveGridInteraction record
     * @param ActiveGridInteraction id
     * @return jobStatus
     */
    public String getJobDescription(int agi_id) {
        String uderDesc = null;

        if (!connect()) {
            _log.fatal("Not connected to database");

            return uderDesc;
        }

        try {
            String sql;

            sql               = "select user_description" + LS + "from ActiveGridInteractions" + LS + "where id = ?;";
            preparedStatement = connect.prepareStatement(sql);
            preparedStatement.setInt(1, agi_id);
            resultSet = preparedStatement.executeQuery();
            resultSet.next();
            uderDesc = resultSet.getString("user_description");
        } catch (SQLException e) {
            _log.fatal(e.toString());
        } finally {
            closeSQLActivity();
        }

        return uderDesc;
    }

    public String getJobStatus(int agi_id) {
        String jobStatus = null;

        if (!connect()) {
            _log.fatal("Not connected to database");

            return jobStatus;
        }

        try {
            String sql;

            sql               = "select status" + LS + "from ActiveGridInteractions" + LS + "where id = ?;";
            preparedStatement = connect.prepareStatement(sql);
            preparedStatement.setInt(1, agi_id);
            resultSet = preparedStatement.executeQuery();

            if (resultSet.next()) {
                ;
            }

            jobStatus = resultSet.getString("status");
        } catch (SQLException e) {
            _log.fatal(e.toString());
        } finally {
            closeSQLActivity();
        }

        return jobStatus;
    }
}

