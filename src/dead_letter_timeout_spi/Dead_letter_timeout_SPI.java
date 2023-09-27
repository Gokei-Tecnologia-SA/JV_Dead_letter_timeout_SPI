package dead_letter_timeout_spi;

import static com.dead_letter_timeout_spi.common.Library.generateRandomAlphanumericString;
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
import entities.MongoDB;
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

    public static String ENVIRONMENT;
    public static String MESSAGE;
    public static String WEBHOOKURL;
    public static String QUERY_STRING;
    public static String DATABASE;
    public static int INTERVAL;
    public static WebhookManager WH_MANAGER;
    public static MongoCollection<Document> COLLECTION_WH;
    public static MongoCollection<Document> COLLECTION_SENDER;
    public static MongoCollection<Document> COLLECTION_WEBHOOK_LOG;
    public static FileWriter FW;
    public static MongoDB MDB;

    public static void main(String[] args) {
        // Cria instancias das dependências
        FW = new FileWriter();
        loadProperties();
        WH_MANAGER = new WebhookManager();
        MDB = new MongoDB();

        // Conecta com banco
        try {
            MongoClient client = MongoClients.create(QUERY_STRING);
            MongoDatabase database = client.getDatabase(DATABASE);
            COLLECTION_SENDER = database.getCollection("sender");
            COLLECTION_WEBHOOK_LOG = database.getCollection("wh_calls");

            System.out.println("Conectado com sucesso ao banco de dados.");
            FW.write("Conectado com sucesso ao banco de dados: " + QUERY_STRING + " no ambiente: " + ENVIRONMENT + " na database: " + DATABASE);

        } catch (Exception ex) {
            System.out.println("Erro ao conectar ao banco de dados: " + ex.getMessage());
            FW.write("Erro ao conectar ao banco de dados: " + QUERY_STRING + " no ambiente: " + ENVIRONMENT + " na database: " + DATABASE);
            System.exit(1);
        }

        // Inicia loop
        while (true) {
            try {
                FindIterable<Document> mensagensNaoEntregues = coletaMensagens();
                for (Document doc : mensagensNaoEntregues) {
                    Document dadosMensagem = doc.get("dados_mensagem", Document.class);
                    Document propriedades = dadosMensagem.get("propriedades", Document.class);

                    String originalInstructionId = propriedades.getString("originalInstructionId");

                    if (originalInstructionId == null) {
                        String endToEnd = propriedades.getString("endToEnd");
                        String returnId = propriedades.getString("returnId");
                        String msgId = propriedades.getString("msgId");

                        if (endToEnd != null) {
                            originalInstructionId = endToEnd;
                        } else if (returnId != null) {
                            originalInstructionId = returnId;
                        } else if (msgId != null) {
                            originalInstructionId = msgId;
                        }
                    }
                
                String getCurrentDate = getCurrentDate(true, true, true, "UTC");
                ObjectId objectId = doc.getObjectId("_id");
                String oid = objectId.toString();

                if (verificaEnvio(oid)) {
                    System.out.println("Mensagem com OID " + oid + " já existe no banco de dados. Pulando o processamento.");
                    //FW.write("Mensagem com OID " + oid + " já existe no banco de dados. Pulando o processamento.");
                    continue;
                }

                System.out.println("Mensagem " + oid + " não encontrada no banco de dados. Enviando mensagem ao Webhook.\n");
                FW.write("Mensagem " + oid + " não encontrada no banco de dados. Enviando mensagem ao Webhook.\n");


                    try {
                        System.out.println("Iniciando conversão de xmlExistente da mensagem com oid: " + oid + "\n");
                        FW.write("Iniciando conversão de xmlExistente da mensagem com oid: " + oid + "\n");
                    String xmlExistente = MESSAGE;
                    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                    DocumentBuilder builder = factory.newDocumentBuilder();
                    org.w3c.dom.Document documentExistente = builder.parse(new InputSource(new StringReader(xmlExistente)));

                    Element refElement = (Element) documentExistente.getElementsByTagName("Ref").item(0);
                    Element creDtElement = (Element) documentExistente.getElementsByTagName("CreDt").item(0);
                    Element rjctnDtTmElement = (Element) documentExistente.getElementsByTagName("RjctnDtTm").item(0);
                    
                    Element bizMsgIdrElement = (Element) documentExistente.getElementsByTagName("BizMsgIdr").item(0);
                    String novoValorBizMsgIdr = generateRandomAlphanumericString();

                    String msgId = "M99999999" +novoValorBizMsgIdr;
                    bizMsgIdrElement.setTextContent(msgId);
                    
                    refElement.setTextContent(originalInstructionId);
                    creDtElement.setTextContent(getCurrentDate);
                    rjctnDtTmElement.setTextContent(getCurrentDate);

                    TransformerFactory tf = TransformerFactory.newInstance();
                    Transformer transformer = tf.newTransformer();
                    StringWriter writer = new StringWriter();
                    transformer.transform(new DOMSource(documentExistente), new StreamResult(writer));
                    String xmlModificado = writer.toString();

                        FW.write("Processando Xml do oid: oid: " + oid + "Do xml: " + xmlModificado + "MsgId do webhook: " + msgId);

                        MessageReporter messageReporter = new MessageReporter();
                        String response = messageReporter.reportMessage(oid, xmlModificado, msgId);

                        // Processamento da resposta do webhook

                        handleWebhookResponse(response);

                        if (WebhookManager.lastHttpResponse != null && WebhookManager.lastHttpResponse.contains("<STATUS>RECEIVED</STATUS>")) {
                            System.out.println("Mensagem " + oid + " enviada com sucesso ao webhook.\n");
                            FW.write("Mensagem " + oid + " enviada com sucesso ao webhook.");
                        }else{
                            System.out.println("Mensagem " + oid + " não foi enviada devido ao erro " + handleWebhookResponse(response) + "\n");
                            FW.write("Mensagem " + oid + " não foi enviada devido ao erro " + handleWebhookResponse(response) + "");
                            continue;
                        }

                    } catch (Exception ex) {
                        ex.printStackTrace();
                        FW.writeException(ex);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                FW.writeException(e);
            }

            try {
                Thread.sleep(60000);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
                FW.writeException(ex);
            }
        }
    }

    private static String handleWebhookResponse(String response) {
        try {
            // Registra o corpo da resposta no log
            System.out.println("Resposta HTTP do Webhook: " + entities.WebhookManager.lastHttpResponse);
            FW.write("Resposta HTTP do Webhook: " + entities.WebhookManager.lastHttpResponse);
        } catch (Exception e) {
            System.out.println("Erro ao processar a resposta do webhook: " + e.getMessage());
            FW.writeException(e);
        }
        return null;
    }

    public static void loadProperties() {
        try (InputStream input = new FileInputStream("properties")) {
            Properties prop = new Properties();
            prop.load(input);

            MESSAGE = prop.getProperty("existent_message");
            ENVIRONMENT = prop.getProperty("ambiente");
            QUERY_STRING = prop.getProperty("query_string_" + ENVIRONMENT);
            DATABASE = prop.getProperty("database_" + ENVIRONMENT);
            WEBHOOKURL = prop.getProperty("webhookurl");

            FW.write("Aplicação iniciada no ambiente: " + ENVIRONMENT);

        } catch (Exception e) {
            System.out.println(e.getMessage());
            FW.writeException(e);
            System.exit(1);
        }
    }

    private static FindIterable<Document> coletaMensagens() {
        Bson filter = eq("dados_mensagem.status", "HTTP_ERROR");
        Bson sort = eq("_id", 1L);
        Bson index = new Document("_id", 1);
        return COLLECTION_SENDER.find(filter).sort(sort).hint(index);
    }
    
    private static boolean verificaEnvio(String oid) {
        Bson filter = eq("mensagem_enviada", oid);
        return COLLECTION_WEBHOOK_LOG.find(filter).first() != null;
    }
    
    private static String getCurrentDate(Boolean comHora, Boolean comDivisoes, Boolean comMilissegundos, String fuso) {
        String formato = "";

        if (comHora && comDivisoes && comMilissegundos)
            formato = "yyyy-MM-dd HH:mm:ss.SSS";
        if (comHora && comDivisoes && !comMilissegundos)
            formato = "yyyy-MM-dd HH:mm:ss";
        if (comHora && !comDivisoes && comMilissegundos)
            formato = "yyyyMMddHHmmssSSS";
        if (comHora && !comDivisoes && !comMilissegundos)
            formato = "yyyyMMddHHmmss";
        if (!comHora && comDivisoes)
            formato = "yyyy-MM-dd";
        if (!comHora && !comDivisoes)
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