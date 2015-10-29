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
    private String ge_status;
    private Date   creation;
    private Date   last_change;
    private String action_info;
    
    private boolean modified_flag;
    
    private static final String LS = System.getProperty("line.separator");        
        
    /**
     * Empty constructor, initialize with dummy/null values
     */
    public GridEngineDaemonCommand() {
        task_id       = -1;
        agi_id        = -1;
        action        = null;
        status        = null;
        ge_status     = null;
        creation      = null;
        last_change   = null;
        action_info   = null;
        modified_flag = false;
    }
    /**
     * Constructor taking all GridEngineDaemon command values
     */
    public GridEngineDaemonCommand(int    task_id
                                  ,int    agi_id
                                  ,String action
                                  ,String status
                                  ,String ge_status
                                  ,Date   creation
                                  ,Date   last_change
                                  ,String action_info) {
        this();
        this.task_id       = task_id;
        this.agi_id        = agi_id;
        this.action        = action;
        this.status        = status;
        this.ge_status     = ge_status;
        this.creation      = creation;
        this.last_change   = last_change;
        this.action_info   = action_info;
        this.modified_flag = false;
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
     * Get GridEngineCommand 'ge_status' field value
     * @return ge_status
     */
    public String getGEStatus() { return this.ge_status; }
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
    public void setTaskId(int task_id){
        if(this.task_id != task_id) modified_flag=true; 
        this.task_id = task_id; 
    }
    /**
     * Set GridEngineCommand 'agi_id' field value
     * @param agi_id
    */
    public void setAGIId(int  agi_id) { 
        if(this.agi_id != agi_id) modified_flag=true; 
        this.agi_id = agi_id;
    }
    /**
     * Set GridEngineCommand 'action' field value
     * @param action
    */
    public void setAction(String action){
         if (action == null) return;
         if(this.action == null || (this.action != null && !this.action.equals(action))) {
             modified_flag=true; 
            this.action = action; 
         }
    }    
    /**
     * Set GridEngineCommand 'status' field value
     * @param status
    */
    public void setStatus(String status){
        if(status == null) return;
        if(this.status == null || (this.status != null && !this.status.equals(status))) {
            modified_flag=true; 
            this.status = status; 
        }
    }
    /**
     * Set GridEngineCommand 'ge_status' field value
     * @param ge_status
    */
    public void setGEStatus(String ge_status){
        if(ge_status == null) return;
        if(this.ge_status == null || (this.ge_status != null && !this.ge_status.equals(ge_status))) {
            modified_flag=true; 
            this.ge_status = ge_status; 
        }
    }
    /**
     * Set GridEngineCommand 'creation' field value
     * @param creation
    */
    public void setCreation(Date creation){ 
        if(creation == null) return;
        if(this.creation == null || (this.creation != null && !this.creation.equals(creation))) {
            modified_flag=true; 
            this.creation = creation; 
        }
    }
    /**
     * Set GridEngineCommand 'last_change' field value
     * @param last_change
    */
    public void setLastChange(Date last_change){ 
        if(last_change == null) return;
        if(this.last_change == null || (this.last_change != null && !this.last_change.equals(last_change))) {
            modified_flag=true; 
            this.last_change=last_change; 
        }
    }
    /**
     * Set GridEngineCommand 'action_info' field value
     * @param action_info
    */
    public void setActionInfo(String action_info){ 
        if(action_info == null) return;
        if(this.action_info == null || (this.action_info != null && !this.action_info.equals(action_info))) {
            modified_flag=true; 
            this.action_info=action_info; 
        }
    }
    
    /**
     * Return true if any field has been modified by any of 'set' methods
     */
    public boolean isModified() { return this.modified_flag; }
    
    /**
     * Reset modified flag
     */
    public void validate() { this.modified_flag = false; }
    
    /**
     * Serialize as string the GridEngineDaemon command values
     */
    @Override
    public String toString() {
        return "{"                                             + LS 
              +"  \"task_id\"      : \"" + task_id       + "\""+ LS
              +"  \"agi_id\"       : \"" + agi_id        + "\""+ LS 
              +"  \"action\"       : \"" + action        + "\""+ LS
              +"  \"status\"       : \"" + status        + "\""+ LS 
              +"  \"ge_status\"    : \"" + ge_status     + "\""+ LS   
              +"  \"creation\"     : \"" + creation      + "\""+ LS
              +"  \"last_change\"  : \"" + last_change   + "\""+ LS
              +"  \"action_info\"  : \"" + action_info   + "\""+ LS
              +"  \"modified_flag\": \"" + modified_flag + "\""+ LS
              +"}"
              ;
    }
}
