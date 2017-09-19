/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package amlt24;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

/**
 *
 * @author embadi
 */
public class AMLT24 {

    String filename;
    String ftpdirectory;
    String table;
    String filenamebefore;
    String remoteFile1;
    String con;
    BufferedReader br;
    int vector_size;
    //String server = "192.168.12.147";
    String server;
    int port = 21;
    //String user = "live";
    String user;
    //String pass = "Kr3p321!";
    String pass;
    FTPClient ftpClient = new FTPClient();
    FTPFile file;
    File downloadFile1;
    long flsize;
    long flsize2;
    PropertyFile prop;
    private static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(AMLT24.class.getName());

    public void process() {
        try {
            // load properties file
            this.prop = new PropertyFile();
            this.prop.Load();
            
            server = this.prop.getT24_server();
            user = this.prop.getT24_uid();
            pass = this.prop.getT24_pwd();
            
            this.ftpClient.connect(this.server, this.port);

            this.ftpClient.login(this.user, this.pass);
            this.ftpClient.enterLocalPassiveMode();
            this.ftpClient.setFileType(2);

            this.remoteFile1 = this.filename;

            FTPFile[] files = this.ftpClient.listFiles(this.ftpClient.printWorkingDirectory() + "/REPORT/AML");

            for (FTPFile file : files) {
                this.filename = file.getName();
                if (this.filename.endsWith("jbase_header")) {
                    continue;
                }

                this.flsize = file.getSize();
                this.remoteFile1 = (this.ftpClient.printWorkingDirectory() + "/REPORT/AML/" + file.getName());
                this.downloadFile1 = new File(this.prop.getAml_path() + "/" + this.filename);

                this.flsize2 = file.getSize();

                log.info("Downloading file... " + this.filename);
                OutputStream outputStream1 = new BufferedOutputStream(new FileOutputStream(this.downloadFile1));
                boolean success = this.ftpClient.retrieveFile(this.remoteFile1, outputStream1);

                outputStream1.close();
                this.ftpClient.deleteFile(this.remoteFile1);
                if (success) {
                    log.info("File " + this.filename + " has been downloaded successfully.");
                }
                this.filenamebefore = this.filename;
            }
            return;
        } catch (IOException ex) {
            log.error(ex);
        } finally {
            if (this.ftpClient.isConnected()) {
                try {
                    this.ftpClient.disconnect();
                } catch (IOException ex) {
                    log.error(ex);
                }
            }
        }
    }

    public static void main(String[] args) {
        AMLT24 ftp2 = new AMLT24();
        ftp2.process();
    }

//    private void moveprocessed() {
//        String timeStamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
//
//        if (this.downloadFile1.renameTo(new File("./Processed/" + timeStamp + "processed" + this.filenamebefore))) {
//            log.info("Processing  file: " + this.filenamebefore + " completed successfully");
//        } else {
//            log.info("Processing  file: " + this.filenamebefore + " not completed successfully");
//        }
//    }

//    private boolean checkdigit(String str) {
//        try {
//            Double.parseDouble(str);
//            return true;
//        } catch (NumberFormatException ex) {
//        }
//        return false;
//    }
}
