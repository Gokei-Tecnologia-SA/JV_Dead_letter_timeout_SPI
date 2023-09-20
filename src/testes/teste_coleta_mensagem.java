package testes;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.eq;
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
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;
import org.xml.sax.InputSource;
import org.bson.BsonNull;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.json.JSONObject;
import org.w3c.dom.Element;
import java.io.StringReader;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;


public class teste_coleta_mensagem {

    private static String ENVIRONMENT;
    private static String QUERY_STRING;
    private static String DATABASE;
    private static String COLLECTION_NAME;
    private static int INTERVAL;
    private static  WebhookManager WH_MANAGER;
    public static  MongoCollection<Document> COLLECTION_WH;
    private static  MongoCollection<Document> COLLECTION_SENDER;
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
    COLLECTION_SENDER = database.getCollection("sender");

    // Inicia loop
    // while(true){
    try {
        System.out.println("#" + getCurrentDate(true, true, true, "BR") + ": Buscando mensagens não entregues...");

        // Coleta todas as mensagens não entregues
        FindIterable<Document> mensagensNaoEntregues = coletaMensagensParaReenvio();
        for (Document doc : mensagensNaoEntregues) {
            // Extrai os campos desejados do documento MongoDB
            Document dadosMensagem = doc.get("dados_mensagem", Document.class);
            if (dadosMensagem != null) {
                Document propriedades = dadosMensagem.get("propriedades", Document.class);
                if (propriedades != null) {
                    String originalInstructionId = propriedades.getString("originalInstructionId");
                        String getCurrentDate = getCurrentDate(true, true, true, "UTC");

                    // Verifique se a String não é nula antes de usá-la
                    if (originalInstructionId != null && getCurrentDate != null) {

                        // Incorpora o código para criar e imprimir o XML modificado
                        try {
                            String xmlExistente = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><Envelope xmlns=\"https://www.bcb.gov.br/pi/admi.002/1.3\"><AppHdr><Fr><FIId><FinInstnId><Othr><Id>99999999</Id></Othr></FinInstnId></FIId></Fr><To><FIId><FinInstnId><Othr><Id>78632767</Id></Othr></FinInstnId></FIId></To><BizMsgIdr>M0000000000000000000000000000000</BizMsgIdr><MsgDefIdr>admi.002.spi.1.3</MsgDefIdr><CreDt>2023-09-02T04:20:12.821Z</CreDt><Sgntr><ds:Signature></ds:Signature></Sgntr></AppHdr><Document><admi.002.001.01><RltdRef><Ref>M78632767Jqq3VuURG51EWgHUx9PA65d</Ref></RltdRef><Rsn><RjctgPtyRsn>Erro no processamento da PSTI</RjctgPtyRsn><RjctnDtTm>2023-09-02T04:20:12.763Z</RjctnDtTm><RsnDesc>Erro ao tentar realizar requisição HTTP ao BACEN</RsnDesc></Rsn></admi.002.001.01></Document></Envelope>";
                            // Cria um novo documento XML com base no XML existente
                            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                            DocumentBuilder builder = factory.newDocumentBuilder();
                            org.w3c.dom.Document documentExistente = builder.parse(new InputSource(new StringReader(xmlExistente)));
                            
                            // Cria o XML modificado
                            // Defina o elemento onde deseja incorporar os dados
                            Element refElement = (Element) documentExistente.getElementsByTagName("Ref").item(0);
                            Element creDtElement = (Element) documentExistente.getElementsByTagName("CreDt").item(0);
                            Element rjctnDtTmElement = (Element) documentExistente.getElementsByTagName("RjctnDtTm").item(0); 

                            // Substitua qualquer conteúdo existente nos elementos com o valor de desejado
                            refElement.setTextContent(originalInstructionId);
                            creDtElement.setTextContent(getCurrentDate);
                            rjctnDtTmElement.setTextContent(getCurrentDate);

                            // Converta o documento DOM modificado de volta para uma string XML
                            TransformerFactory tf = TransformerFactory.newInstance();
                            Transformer transformer = tf.newTransformer();
                            StringWriter writer = new StringWriter();
                            transformer.transform(new DOMSource(documentExistente), new StreamResult(writer));
                            String xmlModificado = writer.toString();
                            System.out.println(xmlModificado);
                            System.out.println();
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                }
            }

            //System.out.println(doc.toJson());
        }
    } catch (Exception e) {
        e.printStackTrace();
    }
    // }
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
            Bson filter = eq("dados_mensagem.status", "HTTP_ERROR");

            Bson sort = eq("_id", 1L);
            Bson index = new Document("_id", 1);
            return COLLECTION_SENDER.find(filter).sort(sort).hint(index);
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
        FindIterable<Document> result = COLLECTION_SENDER.find(filter);
        
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