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
 * Class containing APIServerDaemon commands as registered in 
 * APIServer table asd_queue 
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 */
class APIServerDaemonCommand {
    /*
     APIServer command fields
    */
    private int    task_id;
    private String action;
    private String target;
    private int    target_id;    
    private String status;
    private String target_status;
    private Date   creation;
    private Date   last_change;
    private String action_info;
    
    private boolean modified_flag;
    
    private static final String LS = System.getProperty("line.separator");        
        
    /**
     * Empty constructor, initialize with dummy/null values
     */
    public APIServerDaemonCommand() {
        task_id       = -1;
        target_id     = -1;
        action        = null;
        target        = null;
        status        = null;
        target_status = null;
        creation      = null;
        last_change   = null;
        action_info   = null;
        modified_flag = false;
    }
    /**
     * Constructor taking all GridEngineDaemon command values
     */
    public APIServerDaemonCommand( int    task_id
                                  ,int    target_id
                                  ,String target
                                  ,String action
                                  ,String status
                                  ,String ge_status
                                  ,Date   creation
                                  ,Date   last_change
                                  ,String action_info) {
        this();
        this.task_id       = task_id;
        this.target_id     = target_id;
        this.target        = target;
        this.action        = action;
        this.status        = status;
        this.target_status = ge_status;
        this.creation      = creation;
        this.last_change   = last_change;
        this.action_info   = action_info;
        this.modified_flag = false;
    }        
    
    /**
     * Get APIServerCommand 'task_id' field value
     * @return task_id
     */
    public int getTaskId() { return this.task_id; }
    /**
     * Get APIServerCommand 'target_id' field value
     * @return target_id (For instance GridEngine' Active Grid Interaction id)
     * @see GridEngine' ActiveGridInteraction table
     */
    public int getTargetId() { return this.target_id; }
    /**
     * Get APIServerCommand 'target' field value
     * @return target
     */
    public String getTarget() { return this.target; }
    /**
     * Get APIServerCommand 'action' field value
     * @return action
     */
    public String getAction() { return this.action; }
    /**
     * Get APIServerCommand 'status' field value
     * @return status
     */
    public String getStatus() { return this.status; }
    /**
     * Get APIServerCommand 'target_status' field value
     * @return target_status
     */
    public String getTargetStatus() { return this.target_status; }
    /**
     * Get APIServerCommand 'creation' field value
     * @return creation
     */
    public Date getCreation() { return this.creation; }
    /**
     * Get APIServerCommand 'last_change' field value
     * @return last_change
     */
    public Date getLastChange() { return this.last_change; }
    /**
     * Get APIServerCommand 'action_info' field value
     * @return action_info
     */
    public String getActionInfo() { return this.action_info; }
    
    /**
     * Set APIServerCommand 'task_id' field value
    */
    public void setTaskId(int task_id){
        if(this.task_id != task_id) modified_flag=true; 
        this.task_id = task_id; 
    }
    /**
     * Set APIServerCommand 'target_id' field value
     * @param target_id
    */
    public void setTargetId(int  target_id) { 
        if(this.target_id != target_id) modified_flag=true; 
        this.target_id = target_id;
    }
    /**
     * Set APIServerCommand 'action' field value
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
     * Set APIServerCommand 'status' field value
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
     * Set APIServerCommand 'target_status' field value
     * @param target_status
    */
    public void setTargetStatus(String target_status){
        if(target_status == null) return;
        if(this.target_status == null || (this.target_status != null && !this.target_status.equals(target_status))) {
            modified_flag=true; 
            this.target_status = target_status; 
        }
    }
    /**
     * Set APIServerCommand 'creation' field value
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
     * Set APIServerCommand 'last_change' field value
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
     * Set APIServerCommand 'action_info' field value
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
     * Serialize as string the APIServerDaemon command values
     */
    @Override
    public String toString() {
        return "{"                                             + LS 
              +"  \"task_id\"      : \"" + task_id       + "\""+ LS
              +"  \"target\"       : \"" + target        + "\""+ LS   
              +"  \"target_id\"    : \"" + target_id     + "\""+ LS 
              +"  \"action\"       : \"" + action        + "\""+ LS
              +"  \"status\"       : \"" + status        + "\""+ LS 
              +"  \"target_status\": \"" + target_status + "\""+ LS   
              +"  \"creation\"     : \"" + creation      + "\""+ LS
              +"  \"last_change\"  : \"" + last_change   + "\""+ LS
              +"  \"action_info\"  : \"" + action_info   + "\""+ LS
              +"  \"modified_flag\": \"" + modified_flag + "\""+ LS
              +"}"
              ;
    }
}
