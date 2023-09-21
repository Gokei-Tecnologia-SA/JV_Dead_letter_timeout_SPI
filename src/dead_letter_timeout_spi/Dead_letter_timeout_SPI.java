package dead_letter_timeout_spi;

import java.io.FileInputStream;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import entities.FileWriter;
import entities.MessageReporter;
import com.mongodb.client.FindIterable;
import static com.mongodb.client.model.Filters.eq;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import entities.WebhookManager;
import java.io.StringReader;
import java.io.StringWriter;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.dom.DOMSource;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

public class Dead_letter_timeout_SPI {

    private static String ENVIRONMENT;
    private static String QUERY_STRING;
    private static String DATABASE;
    private static String COLLECTION_NAME;
    private static int INTERVAL;
    private static WebhookManager WH_MANAGER;
    public static MongoCollection<Document> COLLECTION_WH;
    private static MongoCollection<Document> COLLECTION_SENDER;
    private static FileWriter FW;

    public static void main(String[] args) {
        // Cria uma instância de FileWriter
        FW = new FileWriter();
        WH_MANAGER = new WebhookManager();
        loadProperties();

        // Conecta com banco
        MongoClient client = MongoClients.create(QUERY_STRING);
        MongoDatabase database = client.getDatabase(DATABASE);
        COLLECTION_WH = database.getCollection(COLLECTION_NAME);
        COLLECTION_SENDER = database.getCollection("sender");

        // Inicia loop
        while (true) {
            try {
                System.out.println("#" + getCurrentDate(true, true, true, "BR") + ": Buscando mensagens não entregues...");

                // Coleta todas as mensagens não entregues
                FindIterable<Document> mensagensNaoEntregues = coletaMensagens();
                for (Document doc : mensagensNaoEntregues) {
                    Document dadosMensagem = doc.get("dados_mensagem", Document.class);
                    if (dadosMensagem != null) {
                        Document propriedades = dadosMensagem.get("propriedades", Document.class);
                        if (propriedades != null) {
                            String originalInstructionId = propriedades.getString("originalInstructionId");
                            String getCurrentDate = getCurrentDate(true, true, true, "UTC");
                            ObjectId objectId = doc.getObjectId("_id");
                            String oid = objectId.toString();
                            
                            FW.write("Processando mensagem com OID: " + oid);

                            if (originalInstructionId != null && getCurrentDate != null) {
                                try {
                                    String xmlExistente = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><Envelope xmlns=\"https://www.bcb.gov.br/pi/admi.002/1.3\"><AppHdr><Fr><FIId><FinInstnId><Othr><Id>99999999</Id></Othr></FinInstnId></FIId></Fr><To><FIId><FinInstnId><Othr><Id>78632767</Id></Othr></FinInstnId></FIId></To><BizMsgIdr>M0000000000000000000000000000000</BizMsgIdr><MsgDefIdr>admi.002.spi.1.3</MsgDefIdr><CreDt>2023-09-02T04:20:12.821Z</CreDt><Sgntr><ds:Signature></ds:Signature></Sgntr></AppHdr><Document><admi.002.001.01><RltdRef><Ref>M78632767Jqq3VuURG51EWgHUx9PA65d</Ref></RltdRef><Rsn><RjctgPtyRsn>Erro no processamento da PSTI</RjctgPtyRsn><RjctnDtTm>2023-09-02T04:20:12.763Z</RjctnDtTm><RsnDesc>Erro ao tentar realizar requisição HTTP ao BACEN</RsnDesc></Rsn></admi.002.001.01></Document></Envelope>";
                                    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                                    DocumentBuilder builder = factory.newDocumentBuilder();
                                    org.w3c.dom.Document documentExistente = builder.parse(new InputSource(new StringReader(xmlExistente)));

                                    Element refElement = (Element) documentExistente.getElementsByTagName("Ref").item(0);
                                    Element creDtElement = (Element) documentExistente.getElementsByTagName("CreDt").item(0);
                                    Element rjctnDtTmElement = (Element) documentExistente.getElementsByTagName("RjctnDtTm").item(0);

                                    refElement.setTextContent(originalInstructionId);
                                    creDtElement.setTextContent(getCurrentDate);
                                    rjctnDtTmElement.setTextContent(getCurrentDate);

                                    TransformerFactory tf = TransformerFactory.newInstance();
                                    Transformer transformer = tf.newTransformer();
                                    StringWriter writer = new StringWriter();
                                    transformer.transform(new DOMSource(documentExistente), new StreamResult(writer));
                                    String xmlModificado = writer.toString();

                                    MessageReporter messageReporter = new MessageReporter();
                                    messageReporter.reportMessage(oid, xmlModificado);
                                    //System.out.println(xmlModificado);

                                } catch (Exception ex) {
                                    ex.printStackTrace();
                                    FW.writeException(ex);
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                FW.writeException(e);
            }

            try {
                Thread.sleep(INTERVAL);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
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
    
    private static FindIterable<Document> coletaMensagens(){
        Bson filter = eq("dados_mensagem.status", "HTTP_ERROR");
        Bson sort = eq("_id", 1L);
        Bson index = new Document("_id", 1);
        return COLLECTION_SENDER.find(filter).sort(sort).hint(index);
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
