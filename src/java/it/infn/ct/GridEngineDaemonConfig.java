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

import java.util.concurrent.ExecutorService;

/**
 * Class that contain all GridEngineDaemon configuration settings
 * It manages a configuration file and/or static settings
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 * @see GridEngineDaemon
 */
public class GridEngineDaemonConfig {
    /*
       Database settings
     */    
    private String apisrv_dbhost = "localhost";
    private String apisrv_dbport = "3306";
    private String apisrv_dbuser = "geapiserver";
    private String apisrv_dbpass = "geapiserver_password";
    private String apisrv_dbname = "geapiserver";
    
    /*
      GrigEngineDaemon settings
    */
    private int                        gedMaxThreads   =  100;
    private int                        gedCloseTimeout =   20;
    
    /*
      GridEngineDaemonPolling settings
    */
    private int     gePollingDelay       = 4000;
    private int     gePollingMaxCommands =    5;
    
    /*
      GridEngineDaemonController settings
    */
    private int     geControllerDelay       = 4000;
    private int     geControllerMaxCommands =    5;
    
    /*
     GridEngine UsersTracking DB
    */    
    private String utdb_host = "localhost";
    private    int utdb_port = 3306;
    private String utdb_user = "tracking_user";
    private String utdb_pass = "usertracking";
    private String utdb_name = "userstracking";             

    /**
     * Initialize the config object which uses only static settings
     */
    public GridEngineDaemonConfig() { 
    }
    /**
     * Load the given configuration file which overrides static settings
     * @param configFile 
     */
    public GridEngineDaemonConfig(String configFile) { 
        /*
          Load an XML containing GridEngineDaemon settings
        */
    }
    
    /*
      Get and set methods ...
    */

    /*
      Database settings
    */
    
    /**
     * Get GridEngineDaemon database name
     * @return apisrv_dbname
     */
    public String getApisrv_dbname() {
        return apisrv_dbname;
    }
    /**
     * Set GridEngine database name
     * @param apisrv_dbname 
     */
    public void setApisrv_dbname(String apisrv_dbname) {
        this.apisrv_dbname = apisrv_dbname;
    }

    /**
     * Get GridEngineDaemon database host
     * @return apisrv_dbname
     */
    public String getApisrv_dbhost() {
        return apisrv_dbhost;
    }
    /**
     * Set GridEngine database host
     * @param apisrv_dbhost
     */
    public void setApisrv_dbhost(String apisrv_dbhost) {
        this.apisrv_dbhost = apisrv_dbhost;
    }

    /**
     * Get GridEngineDaemon database port
     * @return apisrv_dbport
     */
    public String getApisrv_dbport() {
        return apisrv_dbport;
    }
    /**
     * Set GridEngine database port
     * @param apisrv_dbport
     */
    public void setApisrv_dbport(String apisrv_dbport) {
        this.apisrv_dbport = apisrv_dbport;
    }

    /**
     * Get GridEngineDaemon database user
     * @return apisrv_dbuser
     */
    public String getApisrv_dbuser() {
        return apisrv_dbuser;
    }
    /**
     * Set GridEngine database user
     * @param apisrv_dbuser
     */
    public void setApisrv_dbuser(String apisrv_dbuser) {
        this.apisrv_dbuser = apisrv_dbuser;
    }

    /**
     * Get GridEngineDaemon database password
     * @return apisrv_pass
     */
    public String getApisrv_dbpass() {
        return apisrv_dbpass;
    }
    /**
     * Set GridEngine database password
     * @param apisrv_dbpass
     */
    public void setApisrv_dbpass(String apisrv_dbpass) {
        this.apisrv_dbpass = apisrv_dbpass;
    }

    /**
     * Get GridEngineDaemon thread closure timeout
     * @return gedCloseTimeout number of seconds waiting for thread closure
     */public int getGedCloseTimeout() {
        return gedCloseTimeout;
    }
    /**
     * Set GridEngineDaemon thread closure timeout
     * @param gedCloseTimeout 
     */
    public void setGedCloseTimeout(int gedCloseTimeout) {
        this.gedCloseTimeout = gedCloseTimeout;
    }
        
    /*
      GridEngineDaemon
    */
    
    /**
     * Get GridEngineDaemon max number of threads
     * @return gedMaxThreads maximum number of threads
     */
    public int getMaxThreads() { 
        return gedMaxThreads; 
    }
    /**
     * Set GridEngineDaemon max number of threads
     * @param gedMaxThreads maximum number of threads
     */
    public void setMaxThreads(int gedMaxThreads) { 
        this.gedMaxThreads=gedMaxThreads; 
    }
    
    /**
     * Get GridEngineDaemon closing thread timeout value
     * @return gedMaxThreads maximum number of threads
     */
    public int getCloseTimeout() { 
        return gedCloseTimeout; 
    }
    /**
     * Set GridEngineDaemon closing thread timeout value
     * @param gedCloseTimeout closing thread timeout value
     */
    public void setCloseTimeout(int gedCloseTimeout) { 
        this.gedCloseTimeout=gedCloseTimeout; 
    }
    
    /*
      GridEngineDaemonPolling settings
    */  
    
    /**
     * Get polling thread loop delay
     * @return gePollingDelay number of seconds for each controller loop
     */
    public int getPollingDelay() {
        return gePollingDelay;
    }
    /**
     * Set polling thread loop delay
     * @param gePollingDelay 
     */
    public void setPollingDelay(int gePollingDelay) {
        this.gePollingDelay = gePollingDelay;
    }
    /**
     * Get polling thread max number of commands per loop
     * @return gePollingMaxCommands number of records to be extracted 
     * from ge_queue table
     */
    public int getPollingMaxCommands() {
        return gePollingMaxCommands;
    }
    /**
     * Set polling thread max number of commands per loop
     * @param gePollingMaxCommands 
     */
    public void setPollingMaxCommands(int gePollingMaxCommands) {
        this.gePollingMaxCommands = gePollingMaxCommands;
    }

    /*
      GridEngineDaemonController settings
    */  
    
    /**
     * Get controller thread loop delay
     * @return geControllerDelay number of seconds for each controller loop
     */
    public int getControllerDelay() {
        return geControllerDelay;
    }
    /**
     * Set controller thread loop delay
     * @param geControllerDelay 
     */
    public void setControllerDelay(int geControllerDelay) {
        this.geControllerDelay = geControllerDelay;
    }
    /**
     * Get controller thread max number of commands per loop
     * @return geControllerMaxCommands number of records to be extracted 
     * from ge_queue table
     */
    public int getControllerMaxCommands() {
        return geControllerMaxCommands;
    }
    /**
     * Set controller thread max number of commands per loop
     * @param geControllerMaxCommands 
     */
    public void setControllerMaxCommands(int geControllerMaxCommands) {
        this.geControllerMaxCommands = geControllerMaxCommands;
    }
    
    /*
      GridEngine database settings
    */
    
    /**
     * Return the GridEngine' userstracking database host
     * @return userstracking database host
     */
    String getGridEngine_db_host() { return this.utdb_host; }
    /**
     * Set the GridEngine' userstracking database host
     * @param userstracking database host
     */
    void getGridEngine_db_host(String utdb_host) { this.utdb_host=utdb_host; } 
    /**
     * Return the GridEngine' userstracking database port
     * @return userstracking database port
     */
    int getGridEngine_db_port() { return this.utdb_port; }
    /**
     * Set the GridEngine' userstracking database port
     * @param userstracking database port
     */
    void getGridEngine_db_port(int utdb_port) { this.utdb_port=utdb_port; } 
    /**
     * Return the GridEngine' userstracking database user
     * @return userstracking database user
     */
    String getGridEngine_db_user() { return this.utdb_user; }
    /**
     * Set the GridEngine' userstracking database user
     * @param userstracking database user
     */
    void getGridEngine_db_user(String utdb_user) { this.utdb_user=utdb_user; } 
    /**
     * Return the GridEngine' userstracking database password
     * @return userstracking database password
     */
    String getGridEngine_db_pass() { return this.utdb_pass; }
    /**
     * Set the GridEngine' userstracking database password
     * @param userstracking database password
     */
    void getGridEngine_db_pass(String utdb_pass) { this.utdb_pass=utdb_pass; } 
    /**
     * Return the GridEngine' userstracking database name
     * @return userstracking database name
     */
    String getGridEngine_db_name() { return this.utdb_name; }
    /**
     * Set the GridEngine' userstracking database name
     * @param userstracking database name
     */
    void getGridEngine_db_name(String utdb_name) { this.utdb_name=utdb_name; } 
               
}
