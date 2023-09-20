package entities;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class FileWriter {

    private final String FILE_NAME;
    
    public FileWriter(){
        FILE_NAME = "log.txt";
    }
    
    public FileWriter(String fileName){
        FILE_NAME = fileName;
    }
    
    public void write(String text){
        
        try{
            FileOutputStream fos = new FileOutputStream(FILE_NAME, true);
            DataOutputStream outStream = new DataOutputStream(new BufferedOutputStream(fos));
        
            String header = "#OPN => "+this.getCurrentDate(true, true, true, "BR")+"\n";
            String footer = "\n#CLS => "+this.getCurrentDate(true, true, true, "BR")+"\n\n";
            
            outStream.write(header.getBytes());
            outStream.write(text.getBytes());
            outStream.write(footer.getBytes());
            
            outStream.close();
        } catch (Exception e){
            e.printStackTrace();
        }
        
    }
    
    public void writeException(Exception ex){
        
        try{
            
            PrintWriter pw = new PrintWriter(new FileOutputStream(new File(FILE_NAME), true));
            ex.printStackTrace(pw);
            pw.close();

        } catch(Exception e){
            e.printStackTrace();
        }

    }
    
    //Get curret date and time
    private String getCurrentDate(Boolean comHora, Boolean comDivisoes, Boolean comMilissegundos, String fuso){
        
        String formato = "";
        
        if(comHora && comDivisoes && comMilissegundos)
            formato = "yyyy-MM-dd HH:mm:ss.SSS";
        if(comHora && comDivisoes && !comMilissegundos)
            formato = "yyyy-MM-dd HH:mm:ss";
        if(comHora && !comDivisoes && comMilissegundos)
            formato = "yyyyMMddHHmmssSSS";
        if(comHora && !comDivisoes && !comMilissegundos)
            formato = "yyyyMMddHHmmss";
        if(!comHora && comDivisoes)
            formato = "yyyy-MM-dd";
        if(!comHora && !comDivisoes)
            formato = "yyyyMMdd";
        
        Date date = new Date();
        DateFormat df = new SimpleDateFormat(formato);

        switch (fuso) {
            case "UTC":
                df.setTimeZone(TimeZone.getTimeZone("UTC"));
                break;
            case "BR":
                df.setTimeZone(TimeZone.getTimeZone("America/Sao_Paulo"));
                break;
            default:
                df.setTimeZone(TimeZone.getTimeZone("America/Sao_Paulo"));
                break;
        }
        
        return df.format(date);
        
    }
}
