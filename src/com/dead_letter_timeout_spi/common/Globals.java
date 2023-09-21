package com.dead_letter_timeout_spi.common;

import entities.HSM;
import entities.MongoDB;
import entities.Monitor;
import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Globals {
       
    private final static String PROPERTIES_FILE = "properties.config";
    private final static String PROPERTIES_KEY = "propertiesKey.ser";
    private final static String LOG_LABEL_INFO = "INF";
    private final static String LOG_LABEL_ERROR = "ERR";
    private final static String LOG_LABEL_WARNING = "WRN";
    private final static String LOG_LABEL_DEBUGGER = "DBG";
    private final static String REPORT_TYPE_LABEL_QUESTION = "QUESTION";
    private final static String REPORT_TYPE_LABEL_NOTICE = "NOTICE";
    private final static String REPORT_TYPE_LABEL_REPORT = "REPORT";
    
    private static String VERSION;
    private static String ENVIRONMENT;
    private static boolean DEBUGGING;
    private static String ISPB;
    private static String HSM_USER;
    private static String HSM_PASSWORD;
    private static String HSM_CHAIN_PIC;
    private static String HSM_CHAIN_PIA;
    private static String HSM_PIC_KEY;
    private static String HSM_PIC_CER;
    private static String HSM_PIA_KEY;
    private static String HSM_PIA_CER;
    private static String MONGO_QUERYSTRING;
    private static String MONGO_DATABASE;
    private static String MONGO_COLLECTION_RECEIVER;
    private static String MONGO_COLLECTION_SENDER;
    private static String MONGO_COLLECTION_WEBHOOK;
    private static String MONGO_COLLECTION_RESOURCEIDS;
    private static String MONGO_COLLECTION_FILA_SPI;
    private static String MONGO_ID_USER;
    private static String[] EMERGENCY_NUMBERS;
    private static Integer EMERGENCY_INTERVAL;
    private static int HTTP_COUNTER;
    private static int INTERVAL_HTTP;
    private static int INTERVAL_HTTP_ERROR;
    private static int INTERVAL_ERROR_DB;
    private static int INTERVAL_TOO_MANY_REQUESTS;
    private static Integer TIMEOUT_PIX;
    private static String ICOM_BASE_ENDPOINT;
    private static String ICOM_BASE_ENDPOINT_SENDER;
    private static String INITIAL_PIX_GET_ENDPOINT;
    private static String PIX_HOST;
    private static long LAST_MODIFIED_PROPERTIES;
    private static int LISTENER_THREADPOOL_SIZE;
    private static int MESSAGE_THREADPOOL_SIZE;
    private static int WEBHOOK_TIMEOUT;
    private static String MONITOR_ENDPOINT;
    private static String MONITOR_VOICE_TYPE;
    private static String MONITOR_BINA;
    private static String MONITOR_SPEED;
    private static String MONITOR_TOKEN;
    private static int MONITOR_TIMEOUT;
    private static String REPORT_STATUS;
    private static String REPORT_ENDPOINT;
    private static String REPORT_TOKEN;
    private static String REPORT_USER;
    private static int REPORT_ATTEMPTS;
    private static int REPORT_MAX_TIMETOUT;
    private static HashSet<String> REPORTABLE_MESSSAGES;
    private static boolean USE_CUSTOM_REPORT_WEBHOOK;
            
    private static String PACS002_URL;
    private static String PACS002_VERSION;
    private static String PAIN014_URL;
    private static String PAIN014_VERSION;
    private static String PAIN014_MESSAGE_TYPE;
    private static String ADMI002_STATUS;
    private static String CAMT052_STATUS;
    private static String CAMT053_STATUS;
    private static String CAMT054_STATUS;
    
    private static String FTP_HOST;
    private static int FTP_PORT;
    private static String FTP_USER;
    private static String FTP_PASS;
    private static String FTP_DIR;
    
    private static MongoDB MONGO_DB;
    private static ExecutorService EXECUTOR_SERVICE;
    
    public static void load() {
        
        //Save last file update
        File file = new File(PROPERTIES_FILE);
        LAST_MODIFIED_PROPERTIES = file.lastModified();
        
        Properties props = Library.getProperties(PROPERTIES_FILE);
        
        if(props != null){
            
            ISPB = props.getProperty("ispb");
            VERSION = props.getProperty("version");
            DEBUGGING = Boolean.parseBoolean(props.getProperty("debugging"));
            ENVIRONMENT = props.getProperty("environment");
            if(!ENVIRONMENT.equals("prod") && !ENVIRONMENT.equals("homol")){
                Logger.write(Thread.currentThread().getStackTrace(), LOG_LABEL_WARNING, "Ambiente invalido em properties.config");
                System.exit(1);
            } else {
                Logger.writeError(Thread.currentThread().getStackTrace(), LOG_LABEL_ERROR, "Não foi possível carregar as propriedades do arquivo.");
                System.exit(1);
            }
            
            //Properties
            HSM_USER = props.getProperty("hsm_user_"+ENVIRONMENT);
            HSM_PASSWORD = props.getProperty("hsm_password_"+ENVIRONMENT);
            HSM_CHAIN_PIC = props.getProperty("hsm_pic_bacen_"+ENVIRONMENT);
            HSM_CHAIN_PIA = props.getProperty("hsm_pia_bacen_"+ENVIRONMENT);
            HSM_PIC_KEY = props.getProperty("hsm_pic_key_"+ENVIRONMENT);
            HSM_PIC_CER = props.getProperty("hsm_pic_cer_"+ENVIRONMENT);
            HSM_PIA_KEY = props.getProperty("hsm_pia_key_"+ENVIRONMENT);
            HSM_PIA_CER = props.getProperty("hsm_pia_cer_"+ENVIRONMENT);
            MONGO_QUERYSTRING = props.getProperty("query_string_mdb_"+ENVIRONMENT);
            MONGO_DATABASE = props.getProperty("database_"+ENVIRONMENT);
            MONGO_COLLECTION_RECEIVER = props.getProperty("collection_receiver_"+ENVIRONMENT);
            MONGO_COLLECTION_SENDER = props.getProperty("collection_sender_"+ENVIRONMENT);
            MONGO_COLLECTION_WEBHOOK = props.getProperty("collection_webhook_"+ENVIRONMENT);
            MONGO_COLLECTION_RESOURCEIDS = "resourceIds";
            MONGO_COLLECTION_FILA_SPI = "fila_spi";
            MONGO_ID_USER = props.getProperty("id_user_"+ENVIRONMENT);
            EMERGENCY_NUMBERS = props.getProperty("emergency_numbers").split("@");
            EMERGENCY_INTERVAL = Integer.parseInt(props.getProperty("emergency_call_interval"));
            HTTP_COUNTER = Integer.parseInt(props.getProperty("http_counter"));
            INTERVAL_HTTP = Integer.parseInt(props.getProperty("http_interval"));
            INTERVAL_HTTP_ERROR = Integer.parseInt(props.getProperty("http_interval_error"));
            INTERVAL_TOO_MANY_REQUESTS = Integer.parseInt(props.getProperty("http_interval_too_many_requests"));
            INTERVAL_ERROR_DB = Integer.parseInt(props.getProperty("interval_error_db"));
            TIMEOUT_PIX = Integer.parseInt(props.getProperty("timeout_pix_response"));
            ICOM_BASE_ENDPOINT = ENVIRONMENT.equals("prod") ? "https://icom.pi.rsfn.net.br:16422" : "https://icom-h.pi.rsfn.net.br:16522";
            ICOM_BASE_ENDPOINT_SENDER = ICOM_BASE_ENDPOINT+"/api/v1/in/"+ISPB+"/msgs";
            INITIAL_PIX_GET_ENDPOINT = ICOM_BASE_ENDPOINT+"/api/v1/out/"+ISPB+"/stream/start";
            PIX_HOST = ENVIRONMENT.equals("prod") ? "icom.pi.rsfn.net.br" : "icom-h.pi.rsfn.net.br";
            MONGO_DB = new MongoDB();
            LISTENER_THREADPOOL_SIZE = Integer.parseInt(props.getProperty("threads_listener"));
            MESSAGE_THREADPOOL_SIZE = Integer.parseInt(props.getProperty("threads_message"));
            WEBHOOK_TIMEOUT = Integer.parseInt(props.getProperty("webhook_timeout"));
            MONITOR_ENDPOINT = props.getProperty("monitor_endpoint");
            MONITOR_VOICE_TYPE = props.getProperty("monitor_voice_type");
            MONITOR_BINA = props.getProperty("monitor_bina");
            MONITOR_SPEED = props.getProperty("monitor_speed");
            MONITOR_TOKEN = props.getProperty("monitor_token");
            MONITOR_TIMEOUT = Integer.parseInt(props.getProperty("monitor_timeout"));
            REPORT_STATUS = props.getProperty("report_status_"+ENVIRONMENT);
            REPORT_ENDPOINT = props.getProperty("report_endpoint_"+ENVIRONMENT);
            REPORT_TOKEN = props.getProperty("report_token_"+ENVIRONMENT);
            REPORT_USER = props.getProperty("report_user_"+ENVIRONMENT);
            REPORT_ATTEMPTS = Integer.parseInt(props.getProperty("report_attempts_"+ENVIRONMENT));
            REPORT_MAX_TIMETOUT = Integer.parseInt(props.getProperty("report_max_timeout_"+ENVIRONMENT));
            REPORTABLE_MESSSAGES = Library.generateReportablesMessagesHashSet(props.getProperty("reportable_messages").split("&"));
            USE_CUSTOM_REPORT_WEBHOOK = Boolean.parseBoolean(props.getProperty("report_use_custom_endpoint_"+ENVIRONMENT));
            PACS002_URL = props.getProperty("pacs002_url");
            PACS002_VERSION = props.getProperty("pacs002_version");
            PAIN014_URL = props.getProperty("pain014_url");
            PAIN014_VERSION = props.getProperty("pain014_version");
            PAIN014_MESSAGE_TYPE = props.getProperty("pain014_message_type");
            ADMI002_STATUS = props.getProperty("admi002_translated_status");
            CAMT052_STATUS = props.getProperty("camt052_translated_status");
            CAMT053_STATUS = props.getProperty("camt053_translated_status");
            CAMT054_STATUS = props.getProperty("camt054_translated_status");
            
            FTP_HOST = props.getProperty("ftp_host_"+ENVIRONMENT);
            FTP_PORT = Integer.parseInt(props.getProperty("ftp_port_"+ENVIRONMENT));
            FTP_USER = props.getProperty("ftp_user_"+ENVIRONMENT);
            FTP_PASS = props.getProperty("ftp_pass_"+ENVIRONMENT);
            FTP_DIR = props.getProperty("ftp_dir_"+ENVIRONMENT);
            
            EXECUTOR_SERVICE = Executors.newFixedThreadPool(Integer.parseInt(props.getProperty("threads_listener")));
            
            if(validateProperities()){
                
                String print = "Propriedades carregadas na aplicacao: "
                + "VERSION: "+VERSION+"; "
                + "ISPB: "+ISPB+"; "
                + "ENVIRONMENT: "+ENVIRONMENT+"; "
                + "HSM_USER: "+HSM_USER+"; "
                + "HSM_PASSWORD: "+HSM_PASSWORD+"; "
                + "HSM_CHAIN_PIC: "+HSM_CHAIN_PIC+"; "
                + "HSM_CHAIN_PIA: "+HSM_CHAIN_PIA+"; "
                + "HSM_PIC_KEY: "+HSM_PIC_KEY+"; "
                + "HSM_PIC_CER: "+HSM_PIC_CER+"; "
                + "HSM_PIA_KEY: "+HSM_PIA_KEY+"; "
                + "HSM_PIA_CER: "+HSM_PIA_CER+"; "
                + "MONGO_QUERYSTRING: "+MONGO_QUERYSTRING+"; "
                + "MONGO_DATABASE: "+MONGO_DATABASE+"; "
                + "MONGO_COLLECTION_RECEIVER: "+MONGO_COLLECTION_RECEIVER+"; "
                + "MONGO_COLLECTION_SENDER: "+MONGO_COLLECTION_SENDER+"; "
                + "MONGO_COLLECTION_WEBHOOK: "+MONGO_COLLECTION_WEBHOOK+"; "
                + "MONGO_ID_USER: "+MONGO_ID_USER+"; "
                + "EMERGENCY_NUMBERS: "+Arrays.toString(EMERGENCY_NUMBERS)+";"
                + "EMERGENCY_INTERVAL: "+EMERGENCY_INTERVAL+";"
                + "HTTP_COUNTER: "+HTTP_COUNTER+";"
                + "INTERVAL_HTTP: "+INTERVAL_HTTP+";"
                + "INTERVAL_HTTP_ERROR: "+INTERVAL_HTTP_ERROR+";"
                + "INTERVAL_ERROR_DB: "+INTERVAL_ERROR_DB+";"
                + "INTERVAL_TOO_MANY_REQUESTS: "+INTERVAL_TOO_MANY_REQUESTS+";"
                + "TIMEOUT_PIX: "+TIMEOUT_PIX+"; "
                + "ICOM_BASE_ENDPOINT: "+ICOM_BASE_ENDPOINT+"; "
                + "ICOM_BASE_ENDPOINT_SENDER: "+ICOM_BASE_ENDPOINT_SENDER+"; "
                + "INITIAL_PIX_GET_ENDPOINT: "+INITIAL_PIX_GET_ENDPOINT+"; "
                + "PIX_HOST: "+PIX_HOST+"; "
                + "LAST_MODIFIED_PROPERTIES: "+LAST_MODIFIED_PROPERTIES+"; ";
                
                Logger.write(Thread.currentThread().getStackTrace(), LOG_LABEL_INFO, print);
                System.out.println("# VARIAVEIS LOCAIS INICIALIZADAS");
                
            } else {
                Logger.write(Thread.currentThread().getStackTrace(), LOG_LABEL_WARNING, "Propriedade com valor inválido detecdada");
                System.exit(1);
            }
            
        } else {
            Logger.write(Thread.currentThread().getStackTrace(), LOG_LABEL_WARNING, "Nao foi possivel abrir arquivo de propriedades");
            System.exit(1);
        }
        
    }
    
    private static boolean validateProperities() {
        
        for (Field field : Globals.class.getDeclaredFields()) {
            
            try {
                if (field.get(Globals.class) == null)
                    return false;
            } catch (IllegalAccessException ex) {
                
                Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), "Não foi possível concluir a validação das variáveis globais");
                Logger.writeException(ex);
                System.exit(1);
                
            }
            
        }
        
        return true;
        
    }
    
    public static String getPropertiesKeyName() {
        return PROPERTIES_KEY;
    }
    
    public static String getENVIRONMENT() {
        return ENVIRONMENT;
    }

    public static String getISPB() {
        return ISPB;
    }

    public static String getHSM_USER() {
        return HSM_USER;
    }

    public static String getHSM_PASSWORD() {
        return HSM_PASSWORD;
    }

    public static String getHSM_CHAIN_PIC() {
        return HSM_CHAIN_PIC;
    }

    public static String getHSM_CHAIN_PIA() {
        return HSM_CHAIN_PIA;
    }

    public static String getHSM_PIC_KEY() {
        return HSM_PIC_KEY;
    }

    public static String getHSM_PIC_CER() {
        return HSM_PIC_CER;
    }

    public static String getHSM_PIA_KEY() {
        return HSM_PIA_KEY;
    }

    public static String getHSM_PIA_CER() {
        return HSM_PIA_CER;
    }

    public static String getMONGO_QUERYSTRING() {
        return MONGO_QUERYSTRING;
    }

    public static String getMONGO_DATABASE() {
        return MONGO_DATABASE;
    }

    public static String getMONGO_COLLECTION_RECEIVER() {
        return MONGO_COLLECTION_RECEIVER;
    }

    public static String getMONGO_COLLECTION_SENDER() {
        return MONGO_COLLECTION_SENDER;
    }
    
    public static String getMONGO_COLLECTION_WEBHOOK() {
        return MONGO_COLLECTION_WEBHOOK;
    }
    
    public static String getMONGO_COLLECTION_RESOURCEIDS() {
        return MONGO_COLLECTION_RESOURCEIDS;
    }

    public static String getMONGO_COLLECTION_FILA_SPI() {
        return MONGO_COLLECTION_FILA_SPI;
    }
    
    public static String getMONGO_ID_USER() {
        return MONGO_ID_USER;
    }
    
    public static Integer getTIMEOUT_PIX() {
        return TIMEOUT_PIX;
    }
    
    public static String getICOM_BASE_ENDPOINT() {
        return ICOM_BASE_ENDPOINT;
    }
    
    public static String getICOM_BASE_ENDPOINT_SENDER() {
        return ICOM_BASE_ENDPOINT_SENDER;
    }
    
    public static String getINITIAL_PIX_GET_ENDPOINT() {
        return INITIAL_PIX_GET_ENDPOINT;
    }
    
    public static String getPIX_HOST() {
        return PIX_HOST;
    }
    
    public static MongoDB getMONGODB() {
        return MONGO_DB;
    }
    
    public static HSM getHSM() {
        return new HSM();
    }
    
    public static Monitor getMONITOR() {
        return new Monitor();   
    }
    
    public static String[] getEMERGENCY_NUMBERS() {
        return EMERGENCY_NUMBERS;
    }
    
    public static Integer getEMERGENCY_INTERVAL() {
        return EMERGENCY_INTERVAL;
    }
    
    public static String getLOG_LABEL_INFO() {
        return LOG_LABEL_INFO;
    }

    public static String getLOG_LABEL_ERROR() {
        return LOG_LABEL_ERROR;
    }

    public static String getLOG_LABEL_WARNING() {
        return LOG_LABEL_WARNING;
    }
    
    public static String getLOG_LABEL_DEBUGGER() {
        return LOG_LABEL_DEBUGGER;
    }
    
    public static String getWebhookFromISPB(String ispb, String messageCode) {
        
        Properties props = Library.getProperties(PROPERTIES_FILE);
        if(props != null){
            
            try{
                
                String webhook = props.getProperty("webhook_"+ENVIRONMENT+"_"+ispb+"_"+messageCode);
                return webhook;
            
            } catch(Exception ex) {
                Logger.writeError(Thread.currentThread().getStackTrace(), LOG_LABEL_ERROR, "Nao foi possivel encontrar o webhook para o ispb "+ispb);
                Logger.writeException(ex);
            }
            
        } else
            Logger.write(Thread.currentThread().getStackTrace(), LOG_LABEL_WARNING, "Nao foi possivel abrir arquivo de propriedades");
        
        return null;
        
    }
   
    public static String getPROPERTIES_FILE() {
        return PROPERTIES_FILE;
    }
    
    public static long setLAST_MODIFIED_PROPERTIES(long newTimestamp) {
        
        LAST_MODIFIED_PROPERTIES = newTimestamp;
        return LAST_MODIFIED_PROPERTIES;
        
    }
    
    public static long getLAST_MODIFIED_PROPERTIES() {
        return LAST_MODIFIED_PROPERTIES;
    }
    
    public static int getHTTP_COUNTER() {
        return HTTP_COUNTER;
    }

    public static int getINTERVAL_HTTP() {
        return INTERVAL_HTTP;
    }

    public static int getINTERVAL_HTTP_ERROR() {
        return INTERVAL_HTTP_ERROR;
    }

    public static int getINTERVAL_ERROR_DB() {
        return INTERVAL_ERROR_DB;
    }

    public static int getINTERVAL_TOO_MANY_REQUESTS() {
        return INTERVAL_TOO_MANY_REQUESTS;
    }

    public static String getVERSION() {
        return VERSION;
    }

    public static int getLISTENER_THREADPOOL_SIZE() {
        return LISTENER_THREADPOOL_SIZE;
    }

    public static int getMESSAGE_THREADPOOL_SIZE() {
        return MESSAGE_THREADPOOL_SIZE;
    }

    public static int getWEBHOOK_TIMEOUT() {
        return WEBHOOK_TIMEOUT;
    }

    public static String getMONITOR_ENDPOINT() {
        return MONITOR_ENDPOINT;
    }

    public static String getMONITOR_VOICE_TYPE() {
        return MONITOR_VOICE_TYPE;
    }

    public static String getMONITOR_BINA() {
        return MONITOR_BINA;
    }

    public static String getMONITOR_SPEED() {
        return MONITOR_SPEED;
    }

    public static String getMONITOR_TOKEN() {
        return MONITOR_TOKEN;
    }

    public static int getMONITOR_TIMEOUT() {
        return MONITOR_TIMEOUT;
    }

    public static String getREPORT_STATUS() {
        return REPORT_STATUS;
    }

    public static String getREPORT_ENDPOINT() {
        return REPORT_ENDPOINT;
    }

    public static String getREPORT_TOKEN() {
        return REPORT_TOKEN;
    }
    
    public static String getREPORT_USER() {
        return REPORT_USER;
    }

    public static int getREPORT_ATTEMPTS() {
        return REPORT_ATTEMPTS;
    }

    public static int getREPORT_MAX_TIMETOUT() {
        return REPORT_MAX_TIMETOUT;
    }

    public static HashSet<String> getREPORTABLE_MESSSAGES() {
        return REPORTABLE_MESSSAGES;
    }
    
    public static String getPACS002_URL() {
        return PACS002_URL;
    }

    public static String getPACS002_VERSION() {
        return PACS002_VERSION;
    }

    public static String getPAIN014_URL() {
        return PAIN014_URL;
    }

    public static String getPAIN014_VERSION() {
        return PAIN014_VERSION;
    }

    public static String getPAIN014_MESSAGE_TYPE() {
        return PAIN014_MESSAGE_TYPE;
    }

    public static String getADMI002_STATUS() {
        return ADMI002_STATUS;
    }

    public static String getCAMT052_STATUS() {
        return CAMT052_STATUS;
    }

    public static String getCAMT053_STATUS() {
        return CAMT053_STATUS;
    }

    public static String getCAMT054_STATUS() {
        return CAMT054_STATUS;
    }

    public static String getREPORT_TYPE_LABEL_QUESTION() {
        return REPORT_TYPE_LABEL_QUESTION;
    }

    public static String getREPORT_TYPE_LABEL_NOTICE() {
        return REPORT_TYPE_LABEL_NOTICE;
    }

    public static String getREPORT_TYPE_LABEL_REPORT() {
        return REPORT_TYPE_LABEL_REPORT;
    }
    
    public static boolean isDEBUGGING() {
        return DEBUGGING;
    }

    public static boolean isUSE_CUSTOM_REPORT_WEBHOOK() {
        return USE_CUSTOM_REPORT_WEBHOOK;
    }

    public static String getFTP_HOST() {
        return FTP_HOST;
    }

    public static int getFTP_PORT() {
        return FTP_PORT;
    }

    public static String getFTP_USER() {
        return FTP_USER;
    }

    public static String getFTP_PASS() {
        return FTP_PASS;
    }

    public static String getFTP_DIR() {
        return FTP_DIR;
    }

    public static ExecutorService getEXECUTOR_SERVICE() {
        return EXECUTOR_SERVICE;
    }
    
}