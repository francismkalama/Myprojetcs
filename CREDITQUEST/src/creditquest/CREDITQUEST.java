/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package creditquest;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Vector;
import java.util.logging.Level;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

/**
 *
 * @author FMkalama
 */
public class CREDITQUEST {

    String filename;
    String ftpdirectory;
    String table;
    String filenamebefore;
    String remoteFile1;
    String con;
    BufferedReader br;
    Connection conn = null;
    int vector_size;
    String server = "192.168.12.147";
    int port = 21;
    String user = "live";
    String pass = "Kr3p321!";
    FTPClient ftpClient = new FTPClient();
    FTPFile file;
    File downloadFile1;
    long flsize;
    long flsize2;
    private static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(CREDITQUEST.class.getName());

    public void process() {
        try {
            this.ftpClient.connect(this.server, this.port);

            this.ftpClient.login(this.user, this.pass);
            this.ftpClient.enterLocalPassiveMode();
            this.ftpClient.setFileType(2);

            this.remoteFile1 = this.filename;

            purgeDirectoryButKeepSubDirectories(new File("./Downloaded"));
            FTPFile[] files = this.ftpClient.listFiles(this.ftpClient.printWorkingDirectory() + "/CREDITQUEST");
            System.out.println("Searching CREDITQUEST data in directory " + this.ftpClient.printWorkingDirectory() + "/CREDITQUEST");
            for (FTPFile file : files) {
                this.filename = file.getName();
                this.flsize = file.getSize();
                this.remoteFile1 = (this.ftpClient.printWorkingDirectory() + "/CREDITQUEST/" + file.getName());
                this.downloadFile1 = new File("./Downloaded/" + this.filename);
                System.out.println("CREDITQUEST File found: " + this.filename);
                System.out.println("Checking file size...");
                try {
                    Thread.sleep(60000L);
                } catch (InterruptedException ex) {
                    java.util.logging.Logger.getLogger(CREDITQUEST.class.getName()).log(Level.SEVERE, null, ex);
                }
                this.flsize2 = file.getSize();

                System.out.println("Downloading CREDITQUEST file : " + this.filename);
                OutputStream outputStream1 = new BufferedOutputStream(new FileOutputStream(this.downloadFile1));
                boolean success = this.ftpClient.retrieveFile(this.remoteFile1, outputStream1);

                outputStream1.close();
                this.ftpClient.deleteFile(this.remoteFile1);
                if (success) {
                    System.out.println("File " + this.filename + " has been downloaded successfully.");
                }
                this.filenamebefore = this.filename;
                if (file.getName().toUpperCase().contains("ALL_ACCOUNTS")) {
                    this.table = "[KrepReporting].[dbo].[ALL_ACCOUNTS]";
                    System.out.println("Processing  file: " + this.filename);
                    this.con = "jdbc:sqlserver://192.168.12.6\\sql2008;user=t24;password=t24figt;database=krepreporting";
                    process_csv_file(this.downloadFile1.getAbsolutePath(), this.table, this.con);

                    this.con = "jdbc:sqlserver://192.168.12.231;user=t24_ext;password=Kalc00laz;database=krepreporting";
                    process_csv_file(this.downloadFile1.getAbsolutePath(), this.table, this.con);
                    moveprocessed();
                } else if (file.getName().toUpperCase().contains("LOAN.INS")) {
                    this.table = "[KrepReporting].[dbo].[LOAN_INSURANCE]";
                    System.out.println("Processing  file: " + this.filename);
                    this.con = "jdbc:sqlserver://192.168.12.6\\sql2008;user=t24;password=t24figt;database=krepreporting";
                    process_csv_file(this.downloadFile1.getAbsolutePath(), this.table, this.con);
                    moveprocessed();
                } else if (file.getName().toUpperCase().contains("CLOSED_ACCOUNTS")) {
                    this.table = "[KrepReporting].[dbo].[t_CLOSED_ACCOUNTS]";
                    System.out.println("Processing  file: " + this.filename);
                    this.con = "jdbc:sqlserver://192.168.12.6\\sql2008;user=t24;password=t24figt;database=krepreporting";
                    process_csv_file(this.downloadFile1.getAbsolutePath(), this.table, this.con);
                    moveprocessed();
                } else if (file.getName().toUpperCase().contains("ACCOUNT_ACTIVITY")) {
                    this.table = "[KrepReporting].[dbo].[t_AccountActivity]";
                    System.out.println("Processing  file: " + this.filename);

                    this.con = "jdbc:sqlserver://192.168.12.231;user=t24_ext;password=Kalc00laz;database=krepreporting";
                    process_csv_file(this.downloadFile1.getAbsolutePath(), this.table, this.con);
                    moveprocessed();
                } else if (file.getName().toUpperCase().contains("CATEG.ENTRIES")) {
                    this.table = "[KrepReporting].[dbo].[CATEG.ENTRIES]";
                    System.out.println("Processing  file: " + this.filename);

                    this.con = "jdbc:sqlserver://192.168.12.231;user=t24_ext;password=Kalc00laz;database=krepreporting";
                    process_csv_file(this.downloadFile1.getAbsolutePath(), this.table, this.con);
                    moveprocessed();
                } else if (file.getName().toUpperCase().contains("COLLATERALLIST")) {
                    this.table = "[KrepReporting].[dbo].[COLLATERAL]";
                    System.out.println("Processing  file: " + this.filename);

                    this.con = "jdbc:sqlserver://192.168.12.231;user=t24_ext;password=Kalc00laz;database=krepreporting";
                    process_csv_file(this.downloadFile1.getAbsolutePath(), this.table, this.con);
                    moveprocessed();
                } else if (file.getName().toUpperCase().contains("MD_DEAL")) {
                    this.table = "[KrepReporting].[dbo].[t_Gurantees]";
                    System.out.println("Processing  file: " + this.filename);
                    this.con = "jdbc:sqlserver://192.168.12.6\\sql2008;user=t24;password=t24figt;database=krepreporting";
                    process_csv_file(this.downloadFile1.getAbsolutePath(), this.table, this.con);

                    this.con = "jdbc:sqlserver://192.168.12.231;user=t24_ext;password=Kalc00laz;database=krepreporting";
                    process_csv_file(this.downloadFile1.getAbsolutePath(), this.table, this.con);
                    moveprocessed();
                } else if (file.getName().toUpperCase().contains("OPEN_ACCOUNTS")) {
                    this.table = "[KrepReporting].[dbo].[OPEN_ACCOUNTS]";
                    System.out.println("Processing  file: " + this.filename);
                    this.con = "jdbc:sqlserver://192.168.12.6\\sql2008;user=t24;password=t24figt;database=krepreporting";
                    process_csv_file(this.downloadFile1.getAbsolutePath(), this.table, this.con);
                    moveprocessed();
                } else if (file.getName().toUpperCase().contains("ATM_LD_ANALYSIS")) {
                    this.table = "[KrepReporting].[dbo].[t_LDAnalysis]";
                    System.out.println("Processing  file: " + this.filename);

                    this.con = "jdbc:sqlserver://192.168.12.231;user=t24_ext;password=Kalc00laz;database=krepreporting";
                    process_csv_file(this.downloadFile1.getAbsolutePath(), this.table, this.con);
                    moveprocessed();
                } else if (file.getName().toUpperCase().contains("ATM_DEPOSITSR10")) {
                    this.table = "[KrepReporting].[dbo].[t_LDAnalysisDeposits]";
                    System.out.println("Processing  file: " + this.filename);

                    this.con = "jdbc:sqlserver://192.168.12.231;user=t24_ext;password=Kalc00laz;database=krepreporting";
                    process_csv_file(this.downloadFile1.getAbsolutePath(), this.table, this.con);
                    moveprocessed();
                } else if (file.getName().toUpperCase().contains("CUSTOMER")) {
                    this.table = "[KrepReporting].[dbo].[t_Customer]";
                    System.out.println("Processing  file: " + this.filename);

                    this.con = "jdbc:sqlserver://192.168.12.6\\sql2008;user=t24;password=t24figt;database=krepreporting";
                    process_csv_file(this.downloadFile1.getAbsolutePath(), this.table, this.con);

                    this.con = "jdbc:sqlserver://192.168.12.231;user=t24_ext;password=Kalc00laz;database=krepreporting";
                    process_csv_file(this.downloadFile1.getAbsolutePath(), this.table, this.con);
                    moveprocessed();
                } else if (file.getName().toUpperCase().contains("CURRENCT.RATE")) {
                    this.table = "[KrepReporting].[dbo].[t_Currency]";
                    System.out.println("Processing  file: " + this.filename);

                    this.con = "jdbc:sqlserver://192.168.12.231;user=t24_ext;password=Kalc00laz;database=krepreporting";
                    process_csv_file(this.downloadFile1.getAbsolutePath(), this.table, this.con);
                    System.out.println("Processing  file: " + this.filename);
                    this.con = "jdbc:sqlserver://192.168.12.6\\sql2008;user=t24;password=t24figt;database=krepreporting";
                    process_csv_file(this.downloadFile1.getAbsolutePath(), this.table, this.con);
                    moveprocessed();
                } else if (file.getName().toUpperCase().contains("DAO")) {
                    this.table = "[KrepReporting].[dbo].[t_DAO]";
                    System.out.println("Processing  file: " + this.filename);
                    this.con = "jdbc:sqlserver://192.168.12.6\\sql2008;user=t24;password=t24figt;database=krepreporting";
                    process_csv_file(this.downloadFile1.getAbsolutePath(), this.table, this.con);
                    moveprocessed();
                    String sql = "delete from  " + this.table + " where DAO_Code =''";
                    Statement st = null;
                    try {
                        st = this.conn.createStatement();
                        System.out.println(sql);
                        st.executeUpdate(sql);
                    } catch (SQLException ex) {
                        java.util.logging.Logger.getLogger(CREDITQUEST.class.getName()).log(Level.SEVERE, null, ex);
                    }
                } else if (file.getName().toUpperCase().contains("TODAYS.ENTRIES")) {
                    this.table = "[KrepReporting].[dbo].[t_TODAYSENTRIES]";
                    System.out.println("Processing  file: " + this.filename);
                    this.con = "jdbc:sqlserver://192.168.12.6\\sql2008;user=t24;password=t24figt;database=krepreporting";
                    process_csv_file(this.downloadFile1.getAbsolutePath(), this.table, this.con);
                    moveprocessed();
                } else if (file.getName().toUpperCase().contains("ACCOUNT.ACT")) {
                    this.table = "[KrepReporting].[dbo].[t_AccountActivity_Monthly]";                    
                    this.con = "jdbc:sqlserver://192.168.12.109;user=t24_ext;password=Kalc00laz;database=krepreporting";
                    process_csv_file(this.downloadFile1.getAbsolutePath(), this.table, this.con);
                    moveprocessed();
                } else if (file.getName().toUpperCase().contains("INCOME_YIELD_CQ")) {
                    this.table = "[KrepReporting].[dbo].[CATEG.ENTRIES]";
                    System.out.println("Processing  file: " + this.filename);
                    this.con = "jdbc:sqlserver://192.168.12.109;user=t24_ext;password=Kalc00laz;database=krepreporting";
                    process_csv_file(this.downloadFile1.getAbsolutePath(), this.table, this.con);
                    moveprocessed();
                } 
                else if (file.getName().toUpperCase().contains("REACTIVATED")) {
                    this.table = "[KrepReporting].[dbo].[t_REACTIVATEDACCTS]";
                    System.out.println("Processing  file: " + this.filename);
                    this.con = "jdbc:sqlserver://192.168.12.6\\sql2008;user=t24;password=t24figt;database=krepreporting";
                    process_csv_file(this.downloadFile1.getAbsolutePath(), this.table, this.con);
                    moveprocessed();
                }
                try {
                    this.conn.close();
                    System.out.println("Connection to CREDITQUEST db closed");
                } catch (SQLException ex) {
                    write_to_file(ex);
                }
            }
        } catch (IOException ex) {
            System.out.println("Error: " + ex.getMessage());
            write_to_file(ex);
        } finally {
            if (this.ftpClient.isConnected()) {
                try {
                    this.ftpClient.disconnect();
                } catch (IOException ex) {
                    java.util.logging.Logger.getLogger(CREDITQUEST.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    public static void main(String[] args) {
        CREDITQUEST ftp2 = new CREDITQUEST();
        try {
            for (;;) {
                ftp2.process();
                Thread.sleep(10000L);
            }
        } catch (InterruptedException ex) {
            write_to_file(ex);
        }
    }

    private void process_csv_file(String file, String table, String connection) {
        try {
            try {
                File fl = new File(this.filename);

                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

                this.conn = DriverManager.getConnection(connection);

                System.out.println("Connection to CREDITQUEST db established");
                this.br = new BufferedReader(new FileReader(file));
                String line = "";

                Statement st = this.conn.createStatement();
                int counter = 1;
                while (line != null) {
                    line = this.br.readLine();
                    if (line != null) {
                        line = line.replace("'", " ").trim();

                        String[] det = line.split(",");

                        Vector v = new Vector();
                        for (int x = 0; x < det.length; x++) {
                            v.add(det[x]);
                        }
                        if (counter == 1) {
                            this.vector_size = det.length;
                        }
                        if (v.size() < this.vector_size) {
                            for (int x = v.size(); x < this.vector_size; x++) {
                                v.setSize(this.vector_size);
                                v.setElementAt(" ", x);
                            }
                        }
                        for (int x = v.size(); x < this.vector_size; x++) {
                            if (v.elementAt(x).equals(" ")) {
                                v.setElementAt(Integer.valueOf(0), x);
                            }
                        }
                        int size = det.length;
                        this.filename = this.filename.replace(".csv", "");
                        this.filename = this.filename.toLowerCase();

                        this.filename = this.filename.replace("'", "");
                        if (counter == 1) {
                            String sql = "TRUNCATE TABLE " + table + " ";

                            System.out.println(sql);
                            st.executeUpdate(sql);
                        }
                        String sql = "";
                        System.out.print("\rProcessing CREDITQUEST record..." + String.valueOf(counter - 1));
                        if (counter >= 2) {
                            try {
                                sql = "INSERT INTO " + table + " values (";
                                for (int x = 0; x < v.size() - 1; x++) {
                                    String str = v.elementAt(x).toString();

                                    boolean chk = checkdigit(str);

                                    sql = sql.concat("'" + v.elementAt(x) + "'" + ",");
                                }
                                sql = sql.concat("'" + v.elementAt(v.size() - 1) + "'" + ");");
                                sql = sql.replace(" null", " ");

                                st.executeUpdate(sql);
                            } catch (Exception ex) {
                                log.info(sql);
                                log.error(ex);
                            }
                        }
                        counter++;
                    }
                }
            } catch (FileNotFoundException ex) {
                write_to_file(ex);
            } catch (IOException ex) {
                write_to_file(ex);
            } catch (SQLException ex) {
                write_to_file(ex);
            } catch (ClassNotFoundException ex) {
                write_to_file(ex);
            }
            this.br.close();
        } catch (IOException ex) {
            write_to_file(ex);
        }
    }

    void purgeDirectoryButKeepSubDirectories(File dir) {
        for (File file : dir.listFiles()) {
            if (!file.isDirectory()) {
                file.delete();
            }
        }
    }

    public static void write_to_file(Exception ex) {
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(new File("./exception.txt"), true);
            PrintStream ps = new PrintStream(fos);
            Date date = new Date();
            ps.println("\n");
            ps.println(new Timestamp(date.getTime()));
            ex.printStackTrace(ps);
        } catch (FileNotFoundException ex1) {
            write_to_file(ex1);
        } finally {
            try {
                fos.close();
            } catch (IOException ex1) {
                write_to_file(ex1);
            }
        }
    }

    private void moveprocessed() {
        String timeStamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        System.out.println("\n");
        if (this.downloadFile1.renameTo(new File("./Processed/" + timeStamp + "processed" + this.filenamebefore))) {
            System.out.println("Processing  file: " + this.filenamebefore + " completed successfully");
        } else {
            System.err.println("Processing  file: " + this.filenamebefore + " not completed successfully");
        }
    }

    private boolean checkdigit(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException ex) {
        }
        return false;
    }

}
