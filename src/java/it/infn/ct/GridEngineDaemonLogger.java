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
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * GridEngineDaemon Logging class
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 */
public class GridEngineDaemonLogger {
    
    static Logger logger;
    private Handler fileHandler;
    private Formatter plainText;
    boolean append = false;

    /**
     * Constructor that intialize the GridEngineDaemon logging class
     * it also configures logging characteristics
     * @throws IOException 
     */
    public GridEngineDaemonLogger() throws IOException{
        logger = Logger.getLogger(GridEngineDaemonLogger.class.getName());
        fileHandler = new FileHandler("/Users/Macbook/Documents/FutureGateway/GridEngineDaemon.log",append);
        plainText = new SimpleFormatter();
        fileHandler.setFormatter(plainText);
        logger.addHandler(fileHandler);
    }
    
    /**
     * Printout a given logging message at the given logging level
     * @param level
     * @param msg 
     */
    private static void log(Level level, String msg){
        logger.log(level, msg);
        System.out.println(msg);
    }
    
    /**
     * Printout a given message at info log level
     * @param msg 
     */
    public static void info(String msg) { log(Level.INFO  ,msg); }
    /**
     * Printout a given message at severe log level
     * @param msg 
     */
    public static void severe(String msg) { log(Level.SEVERE ,msg); }
    /**
     * Printout a given message at warning log level
     * @param msg 
     */    
    public static void warning(String msg) { log(Level.WARNING,msg); }
}
