package dead_letter_spi;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import entities.FileWriter;
import entities.WebhookManager;
import java.io.FileInputStream;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;
import org.bson.BsonNull;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.json.JSONException;
import org.json.JSONObject;

public class Dead_letter_SPI {

    private static String ENVIRONMENT;
    private static String QUERY_STRING;
    private static String DATABASE;
    private static String COLLECTION_NAME;
    private static int INTERVAL;
    private static  WebhookManager WH_MANAGER;
    public static  MongoCollection<Document> COLLECTION_WH;
    private static  MongoCollection<Document> COLLECTION_RECEIVER;
    private static FileWriter FW;
    
    public static void main(String[] args) {
        
        // Carrega propriedades
        FW = new FileWriter();
        WH_MANAGER = new WebhookManager();
        loadProperties();
        
        // Conecta com banco
        MongoClient client = MongoClients.create(QUERY_STRING);
        MongoDatabase database = client.getDatabase(DATABASE);
        COLLECTION_WH = database.getCollection(COLLECTION_NAME);
        COLLECTION_RECEIVER = database.getCollection("receiver");
        
        // Inicia loop
        while(true){
        
            System.out.println("#"+getCurrentDate(true, true, true, "BR")+": Buscando mensagens não entregues...");
            
            // Coleta todas as mensagens não entregues
            FindIterable<Document> mensagensNaoEntregues = coletaMensagensParaReenvio();
            for(Document doc : mensagensNaoEntregues){
                try{
                    reenviaMensagem(new JSONObject(doc.toJson()));   
                } catch(JSONException ex){
                    FW.writeException(ex);
                }
            }
           
            // Aguarda próxima interação
            try{
                Thread.sleep(INTERVAL);
            } catch(InterruptedException ex){
                FW.writeException(ex);
            }
        
        }
        
    }
    
    private static void loadProperties(){
        
        try(InputStream input = new FileInputStream("properties.config")){

            Properties prop = new Properties();
            prop.load(input);
            
            ENVIRONMENT = prop.getProperty("ambiente");
            QUERY_STRING = prop.getProperty("query_string_"+ENVIRONMENT);
            DATABASE = prop.getProperty("database_"+ENVIRONMENT);
            COLLECTION_NAME = "wh_calls";
            INTERVAL = Integer.parseInt(prop.getProperty("interval_ms"));
            FW.write("Aplicação iniciada no ambiente: "+ENVIRONMENT);
            
        } catch(Exception e){
            System.out.println(e.getMessage());
            FW.writeException(e);
            System.exit(1);
        }
        
    }
    
    private static FindIterable<Document> coletaMensagensParaReenvio(){
        
        Bson filter = and(Arrays.asList(
            eq("id_resposta", new BsonNull()),
            gte("tentativa", 5L),
            eq("tipo", "REPORT")
        ));

        Bson sort = eq("_id", 1L);
        Bson index = new Document("_id", 1);
        return COLLECTION_WH.find(filter).sort(sort).hint(index);
        
    }
    
    private static void reenviaMensagem(JSONObject chamada){
            
        try{
           
            // Coleta os dados para tentar refazer a chamada
            String endpoint       = chamada.getString("endpoint");
            String header         = chamada.getJSONObject("http").getJSONObject("request").getString("head");
            String message        = chamada.getJSONObject("http").getJSONObject("request").getString("body");
            String idempotencyKey = UUID.randomUUID().toString();
            String whId           = chamada.getJSONObject("_id").getString("$oid");
            int tentativaAtual    = chamada.getInt("tentativa");
            String messageId      = null;
            String messageCode    = null;
            String messageVersion = null;
            
            // Extrai dados do cabeçalho anterior
            String[] dadosCabecalho = header.split(",");
            for(String dado : dadosCabecalho){
                
                String[] infoDado = dado.split("=");
                switch(infoDado[0].replace("{", "").replace("}", "").trim()){
                    case "spi-message-id":
                        messageId = infoDado[1].replace("[", "").replace("]", "").replace("{", "").replace("}", "").trim();
                        break;
                    case "spi-message-code": 
                        messageCode = infoDado[1].replace("[", "").replace("]", "").replace("{", "").replace("}", "").trim();
                        break;
                    case "spi-message-version":
                        messageVersion = infoDado[1].replace("[", "").replace("]", "").replace("{", "").replace("}", "").trim();
                        break;
                }
                
            }
            
            if(messageId != null && messageCode != null && messageVersion != null){
                
                FW.write("Iniciando reenvio da mensagem com mongoId "+whId);
                System.out.println("Reenviando mensagem "+whId);

                // Tenta refazer a chamada
                WH_MANAGER.sendRequest(whId, tentativaAtual, endpoint, message, idempotencyKey, messageId, messageCode, messageVersion);
                
            } else {
                FW.write("Não foi possível extrair os dados no cabeçalho da mensagem com mongoId "+whId);
            }
            
        } catch(Exception ex) {
            FW.writeException(ex);
        }
        
    }
    
    private static String getSavedDate(String mongoId){
        
        String date = "0000-00-00T00:00:00.000Z";
        
        Bson filter = eq("_id", new ObjectId(mongoId));
        FindIterable<Document> result = COLLECTION_RECEIVER.find(filter);
        
        for(Document doc : result){
            
            JSONObject message = new JSONObject(doc.toJson());
            
            try{
                
                String currentDate = message.getString("data_gmt");
                DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
                LocalDateTime localDateTime = LocalDateTime.parse(currentDate, inputFormatter);
                ZoneId brazilZoneId = ZoneId.of("America/Sao_Paulo");
                OffsetDateTime offsetDateTime = localDateTime.atZone(brazilZoneId).toOffsetDateTime();
                DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                return offsetDateTime.format(outputFormatter);
                
            }catch(Exception ex){
                FW.writeException(ex);
            }
            
        }
        
        return date;
        
    }

    private static String getCurrentDate(Boolean comHora, Boolean comDivisoes, Boolean comMilissegundos, String fuso){
        
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
