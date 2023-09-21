package com.dead_letter_timeout_spi.common;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Level;

public class Logger {
    
    private static String DIRECTORY = "historic/"+Library.getYear()+"/"+Library.getMonth()+"/"+Library.getDay();
    private static String LOG_FILE;

    private static void checkDirectory() {
        
        DIRECTORY = "historic/"+Library.getYear()+"/"+Library.getMonth()+"/"+Library.getDay();
        Path path = Paths.get(DIRECTORY);
        if (!Files.exists(path)) {
            try {
                Files.createDirectories(path);
            } catch (IOException ex) {
                java.util.logging.Logger.getLogger(Logger.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
        LOG_FILE = DIRECTORY+"/log.txt";
        
    }
    
    public static void write(StackTraceElement[] stackTrace, String type, String text) {
        
        checkDirectory();
        try {

            DataOutputStream dos = getDataOutputStream();
            
            if(dos != null){
                
                writeIndex(dos, stackTrace, type);
                dos.write((Library.removeQuebrasLinha(text)+"\n").getBytes());
                dos.close();
                
            }

        } catch (IOException ex) {

            java.util.logging.Logger.getLogger(Logger.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);

        }
        
        
    }
   
    public static void writeException(Exception ex) {

        checkDirectory();
        PrintWriter pw = getPrintWriter();
        if(pw != null){
            
            ex.printStackTrace(pw);
            boolean print = false;
            if(print){
                printException(ex);
            }
            pw.close();
            
        }

    }
    
    public static void writeError(StackTraceElement[] stackTrace, String type, String text){
        
        checkDirectory();
        try {

            DataOutputStream dos = getDataOutputStream();
            
            if(dos != null){
                      
                writeIndex(dos, stackTrace, type);
                dos.write((Library.removeQuebrasLinha(text)+"\n").getBytes());
                dos.flush();
                dos.close();
                
                System.out.println(text);
                
            }

        } catch (IOException ex) {

            java.util.logging.Logger.getLogger(Logger.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);

        }
        
    }
    
    public static void printException(Exception ex){
        java.util.logging.Logger.getLogger(Logger.class.getName()).log(Level.SEVERE, null, ex);
    }
    
    public static void printHTTPRequestInfo(String url, String method, String header){
        
        String request =
            "----------\n" +
            "- Date: "+Library.getDate(true, true, true, true, "BR")+"\n"+
            "- Request: ("+method+") "+url+"\n"+
            "-----------";
        
        System.out.println(request);
        
    }
    
    private static DataOutputStream getDataOutputStream(){
        
        try {

            FileOutputStream fos     = new FileOutputStream(LOG_FILE, true);
            BufferedOutputStream bos = new BufferedOutputStream(fos);
            DataOutputStream dos     = new DataOutputStream(bos);
            return dos;
            
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(Logger.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
        
    }
    
    private static PrintWriter getPrintWriter(){
        
        try {
            
            File file            = new File(LOG_FILE);
            FileOutputStream fos = new FileOutputStream(file, true);
            PrintWriter pw       = new PrintWriter(fos);
            return pw;
            
        } catch(FileNotFoundException ex) {
            
            java.util.logging.Logger.getLogger(Logger.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
            return null;
            
        }
        
    }
    
    private static void writeIndex(DataOutputStream dos, StackTraceElement[] stackTrace, String type) {
        
        try {
            
            String date = Library.getDate(true, true, true, true, "UTC");
            StackTraceElement element = stackTrace[1];
            String classe = element.getClassName();
            String metodo = element.getMethodName();
            int linha = element.getLineNumber();
            
            dos.write(("#"+type+"|"+date+"|"+classe+"|"+metodo+"|"+linha+" => ").getBytes());
            
        } catch (IOException ex) {
            
            java.util.logging.Logger.getLogger(Logger.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        
        }
        
    }

    public static void writeInfo(StackTraceElement[] stackTrace, String log_label_info, String string) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }
    
}