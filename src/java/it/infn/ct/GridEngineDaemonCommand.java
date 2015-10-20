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

import java.sql.Date;

/**
 * Class containing GridEngineDaemon commands as registered in 
 * GridEngine APIServer table ge_queue 
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 */
class GridEngineDaemonCommand {
    /*
     GridEngine APIServer command fields
    */
    private int    task_id;
    private int    agi_id;
    private String action;
    private String status;
    private Date   creation;
    private Date   last_change;
    private String action_info;
    
    private static final String LS = System.getProperty("line.separator");        
        
    /**
     * Empty constructor, initialize with dummy/null values
     */
    public GridEngineDaemonCommand() {
        task_id     = -1;
        agi_id      = -1;
        action      = null;
        status      = null;
        creation    = null;
        last_change = null;
        action_info = null;
    }
    /**
     * Constructor taking all GridEngineDaemon command values
     */
    public GridEngineDaemonCommand(int    task_id
                                  ,int    agi_id
                                  ,String action
                                  ,String status
                                  ,Date   creation
                                  ,Date   last_change
                                  ,String action_info) {
        this.task_id     = task_id;
        this.agi_id      = agi_id;
        this.action      = action;
        this.status      = status;
        this.creation    = creation;
        this.last_change = last_change;
        this.action_info = action_info;
    }        
    
    /**
     * Get GridEngineCommand 'task_id' field value
     * @return task_id
     */
    public int getTaskId() { return this.task_id; }
    /**
     * Get GridEngineCommand 'agi_id' field value
     * @return agi_id (agi - Active Grid Interaction)
     * @see GridEngine' ActiveGridInteraction table
     */
    public int getAGIId() { return this.agi_id; }
    /**
     * Get GridEngineCommand 'action' field value
     * @return action
     */
    public String getAction() { return this.action; }
    /**
     * Get GridEngineCommand 'status' field value
     * @return status
     */
    public String getStatus() { return this.status; }
    /**
     * Get GridEngineCommand 'creation' field value
     * @return creation
     */
    public Date getCreation() { return this.creation; }
    /**
     * Get GridEngineCommand 'last_change' field value
     * @return last_change
     */
    public Date getLastChange() { return this.last_change; }
    /**
     * Get GridEngineCommand 'action_info' field value
     * @return action_info
     */
    public String getActionInfo() { return this.action_info; }
    
    /**
     * Set GridEngineCommand 'task_id' field value
    */
    public void setTaskId(int task_id){this.task_id = task_id; }
    /**
     * Set GridEngineCommand 'agi_id' field value
     * @param agi_id
    */
    public void setAGIId(int  agi_id){ this.agi_id = agi_id; }
    /**
     * Set GridEngineCommand 'action' field value
     * @param action
    */
    public void setAction(String action){ this.action = action; }
    /**
     * Set GridEngineCommand 'status' field value
     * @param status
    */
    public void setStatus(String status){ this.status = status; }
    /**
     * Set GridEngineCommand 'creation' field value
     * @param creation
    */
    public void setCreation(Date creation){ this.creation = creation; }
    /**
     * Set GridEngineCommand 'last_change' field value
     * @param last_change
    */
    public void setLastChange(Date last_change){ this.last_change=last_change; }
    /**
     * Set GridEngineCommand 'action_info' field value
     * @param action_info
    */
    public void setActionInfo(String action_info){ this.action_info=action_info; }
    
    /**
     * Serialize as string the GridEngineDaemon command values
     */
    @Override
    public String toString() {
        return "{"                                          + LS 
              +"  \"task_id\"     : \"" + task_id     + "\""+ LS
              +"  \"agi_id\"      : \"" + agi_id      + "\""+ LS 
              +"  \"action\"      : \"" + action      + "\""+ LS
              +"  \"status\"      : \"" + status      + "\""+ LS 
              +"  \"creation\"    : \"" + creation    + "\""+ LS
              +"  \"last_change\" : \"" + last_change + "\""+ LS
              +"  \"action_info\" : \"" + action_info + "\""+ LS
              +"}"
              ;
    }
}
