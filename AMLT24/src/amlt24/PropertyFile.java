/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package amlt24;

import java.io.InputStream;
import java.util.Properties;

/**
 *
 * @author mbadi
 */
public class PropertyFile {

    private String aml_path;
    private String t24_server;
    private String t24_uid;
    private String t24_pwd;

    private Properties prop;
    private InputStream input;
    private static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(PropertyFile.class.getName());

    public String getAml_path() {
        return aml_path;
    }

    public String getT24_server() {
        return t24_server;
    }

    public String getT24_uid() {
        return t24_uid;
    }

    public String getT24_pwd() {
        return t24_pwd;
    }

    public boolean Load() {

        boolean retval = false;

        try {
            log.info("Properties Loading ...");

            this.prop = new Properties();

            String loginfo = "";
            String file = "config.properties";
            input = this.getClass().getClassLoader().getResourceAsStream(file);

            prop.load(input);

            this.aml_path = prop.getProperty("aml_path");
            loginfo += "aml_path: " + aml_path;

            this.t24_server = prop.getProperty("t24_server");
            loginfo += "#t24_server: " + t24_server;

            this.t24_uid = prop.getProperty("t24_uid");
            loginfo += "#t24_uid: " + t24_uid;

            this.t24_pwd = prop.getProperty("t24_pwd");
            loginfo += "#t24_pwd: " + t24_pwd;

            if (!loginfo.equals("")) {
                log.info(loginfo);
            }

            retval = true;

        } catch (Exception ex) {
            log.error(ex);
        }

        return retval;
    }
}
