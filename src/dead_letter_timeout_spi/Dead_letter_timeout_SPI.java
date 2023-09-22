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
                String currentDate = getCurrentDate(true, true, true, "BR");
                System.out.println("#" + currentDate + ": Buscando mensagens não entregues...");
                FW.write("#" + currentDate + ": Buscando mensagens não entregues...");

                // Coleta todas as mensagens não entregues
                FindIterable<Document> mensagensNaoEntregues = coletaMensagens();
                for (Document doc : mensagensNaoEntregues) {
                    Document dadosMensagem = doc.get("dados_mensagem", Document.class);
                    if (dadosMensagem != null) {
                        Document propriedades = dadosMensagem.get("propriedades", Document.class);
                        if (propriedades != null) {
                            //FW.write("JSON encontrado: " + doc.toJson());

                            System.out.println("Iniciando fase de processamento");
                            FW.write("Iniciando fase de processamento");

                            String originalInstructionId = propriedades.getString("originalInstructionId");
                            String getCurrentDate = getCurrentDate(true, true, true, "UTC");
                            ObjectId objectId = doc.getObjectId("_id");
                            String oid = objectId.toString();

                            System.out.println("Processando mensagem com OID: " + oid);
                            FW.write("Processando mensagem com OID: " + oid + "\nMais JSON:" + doc.toJson());

                            if (originalInstructionId != null && getCurrentDate != null) {
                                try {
                                    System.out.println("Iniciando conversão de xmlExistente");
                                    FW.write("Iniciando conversão de xmlExistente");
                                    String xmlExistente = MESSAGE;
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

                                    FW.write("Processando Xml: " + xmlModificado);

                                    MessageReporter messageReporter = new MessageReporter();
                                    String response = messageReporter.reportMessage(oid, xmlModificado);

                                    // Processamento da resposta do webhook
                                    handleWebhookResponse(response);

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

    private static void handleWebhookResponse(String response) {
        try {
            System.out.println("Resposta HTTP do Webhook: " + entities.WebhookManager.lastHttpResponse);
            FW.write("Resposta HTTP do Webhook: " + entities.WebhookManager.lastHttpResponse);
            // Registra o corpo da resposta no log
            System.out.println("Resposta do Webhook: " + response);
            FW.write("Id de retorno Webhook: " + response);
        } catch (Exception e) {
            System.out.println("Erro ao processar a resposta do webhook: " + e.getMessage());
            FW.writeException(e);
        }
    }

    public static void loadProperties() {
        try (InputStream input = new FileInputStream(".gitignore")) {
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