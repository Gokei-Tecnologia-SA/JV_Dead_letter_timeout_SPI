package entities;

import com.dead_letter_timeout_spi.common.Globals;
import com.dead_letter_timeout_spi.common.Library;
import com.dead_letter_timeout_spi.common.Logger;
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import org.bson.Document;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.elemMatch;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Filters.or;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
import com.dead_letter_timeout_spi.models.InsertResult;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Projections.include;
import java.util.List;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class MongoDB {

    private final MongoClient CLIENT;
    private final MongoDatabase DATABASE;
    private final MongoCollection<Document> COLLECTION_RECEIVER;
    private final MongoCollection<Document> COLLECTION_SENDER;
    private final MongoCollection<Document> COLLECTION_WEBHOOK_LOG;
    private final MongoCollection<Document> COLLECTION_RESOURCEIDS;
    private final MongoCollection<Document> COLLECTION_FILA_SPI;

    public MongoDB() {
        CLIENT = MongoClients.create(Globals.getMONGO_QUERYSTRING());
        DATABASE = CLIENT.getDatabase(Globals.getMONGO_DATABASE());
        COLLECTION_RECEIVER = DATABASE.getCollection(Globals.getMONGO_COLLECTION_RECEIVER());
        COLLECTION_SENDER = DATABASE.getCollection(Globals.getMONGO_COLLECTION_SENDER());
        COLLECTION_WEBHOOK_LOG = DATABASE.getCollection(Globals.getMONGO_COLLECTION_WEBHOOK());
        COLLECTION_RESOURCEIDS = DATABASE.getCollection(Globals.getMONGO_COLLECTION_RESOURCEIDS());
        COLLECTION_FILA_SPI = DATABASE.getCollection(Globals.getMONGO_COLLECTION_FILA_SPI());
    }

    public boolean testConnection() {
        try {
            MongoDatabase database = CLIENT.getDatabase(Globals.getMONGO_DATABASE());
            return database.listCollectionNames() != null;
        } catch (Exception ex) {
            Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), "Erro ao tentar se conectar com o MongoDB");
            Logger.writeException(ex);
            return false;
        }
    }

    public boolean saveResourceID(String resourceId) {
        try {
            Document doc = new Document().append("resourceId", resourceId);
            InsertOneResult result = COLLECTION_RESOURCEIDS.insertOne(doc);
            return result.getInsertedId() != null;
        } catch (Exception ex) {
            Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), "Erro ao salvar resourceId no MongoDB");
            Logger.writeException(ex);
            return false;
        }
    }

    public MongoClient getCLIENT() {
        return CLIENT;
    }

    public MongoDatabase getDATABASE() {
        return DATABASE;
    }

    public MongoCollection<Document> getCOLLECTION_RECEIVER() {
        return COLLECTION_RECEIVER;
    }

    public MongoCollection<Document> getCOLLECTION_SENDER() {
        return COLLECTION_SENDER;
    }

    public MongoCollection<Document> getCOLLECTION_WEBHOOK_LOG() {
        return COLLECTION_WEBHOOK_LOG;
    }

    public ArrayList<InsertResult> saveMessageInDB(String validationStatus, String piResourceId, JSONObject httpData, JSONObject message) {
        ArrayList<InsertResult> newIds = new ArrayList<>();
        BsonValue temp = null;
        
        try{

            //Base information
            String inf_dataGmt = Library.getDate(true, true, true, false, "BR");
            String inf_dataUtc = Library.getDate(true, true, true, false, "UTC");

            //HTTP data
            int    http_code = Library.getIntFromJson(httpData, "code");
            String http_head = Library.getStringFromJson(httpData, "head");
            String http_body = Library.getStringFromJson(httpData, "body");
            int    http_time = Library.getIntFromJson(httpData, "time");

            //Received message
            String xml = Library.getStringFromJson(httpData, "body");

            //Message info
            String msg_version = null;
            String msg_code = null;
            String msg_type = null;
            String msg_messageId = null;
            String msg_endToEndId = null;
            String msg_returnId = null;
            String msg_originalEndToEndId = null;
            String msg_originalInstructionId = null;
            String msg_originalMessageId = null;
            String msg_originalResourceId = null;
            String msg_scope = null;
            String msg_originISPB = null;
            String msg_destinyISPB = null;

            if(message != null){

                // Single messages info
                msg_scope                 = Library.getStringFromJson(message, "scope");
                msg_originISPB            = Library.getStringFromJson(message, "originISPB");
                msg_destinyISPB           = Library.getStringFromJson(message, "destinyISPB");
                msg_version               = Library.getStringFromJson(message, "version");
                msg_code                  = Library.getStringFromJson(message, "code");
                msg_type                  = Library.getStringFromJson(message, "type");
                msg_messageId             = Library.getStringFromJson(message, "messageId");

            }

            if(msg_code != null){

                int size;
                switch (msg_code) {
                    case "admi002":
                        msg_endToEndId            = null;
                        msg_returnId              = null;
                        msg_originalEndToEndId    = null;
                        msg_originalInstructionId = null;
                        msg_originalMessageId     = Library.getStringFromJson(message, "originalMessageId");
                        msg_originalResourceId    = Library.getStringFromJson(message, "originalResourceId");
                        temp = this.saveReceivedMessage(
                            xml,
                            http_code,
                            http_head,
                            http_body,
                            http_time,
                            piResourceId,
                            msg_version,
                            msg_code,
                            msg_type,
                            msg_messageId,
                            msg_endToEndId,
                            msg_returnId,
                            msg_originalEndToEndId,
                            msg_originalInstructionId,
                            msg_originalMessageId,
                            msg_originalResourceId,
                            validationStatus,
                            inf_dataGmt,
                            inf_dataUtc,
                            msg_originISPB,
                            msg_destinyISPB,
                            msg_scope,
                            false
                        );
                        newIds.add(new InsertResult(msg_originalResourceId, temp, msg_originalResourceId, msg_originalMessageId, msg_originalInstructionId, msg_originalEndToEndId));
                        break;
                      
                    case "pacs008":
                        if(message != null){
                            size = message.getJSONArray("payments").length();
                            for(int i = 0; i < size; i++){
                                JSONObject data = new JSONObject(message.getJSONArray("payments").get(i).toString());
                                msg_endToEndId            = Library.getStringFromJson(data, "endToEndId");
                                msg_returnId              = null;
                                msg_originalEndToEndId    = null;
                                msg_originalInstructionId = null;
                                msg_originalMessageId     = null;
                                msg_originalResourceId    = null;
                                temp = this.saveReceivedMessage(
                                    xml,
                                    http_code,
                                    http_head,
                                    http_body,
                                    http_time,
                                    piResourceId,
                                    msg_version,
                                    msg_code,
                                    msg_type,
                                    msg_messageId,
                                    msg_endToEndId,
                                    msg_returnId,
                                    msg_originalEndToEndId,
                                    msg_originalInstructionId,
                                    msg_originalMessageId,
                                    msg_originalResourceId,
                                    validationStatus,
                                    inf_dataGmt,
                                    inf_dataUtc,
                                    msg_originISPB,
                                    msg_destinyISPB,
                                    msg_scope,
                                    size > 1
                                );
                                newIds.add(new InsertResult(msg_endToEndId, temp, msg_originalResourceId, msg_originalMessageId, msg_originalInstructionId, msg_originalEndToEndId));
                            }
                        }
                        break;
                        
                    case "pacs002":
                        if(message != null){
                            size = message.getJSONArray("transactions").length();
                            for(int i = 0; i < size; i++){
                                JSONObject data = new JSONObject(message.getJSONArray("transactions").get(i).toString());
                                msg_endToEndId            = null;
                                msg_returnId              = null;
                                msg_originalEndToEndId    = Library.getStringFromJson(data, "originalEndToEndId");
                                msg_originalInstructionId = Library.getStringFromJson(data, "originalInstructionId");
                                msg_originalMessageId     = null;
                                msg_originalResourceId    = null;
                                temp = this.saveReceivedMessage(
                                    xml,
                                    http_code,
                                    http_head,
                                    http_body,
                                    http_time,
                                    piResourceId,
                                    msg_version,
                                    msg_code,
                                    msg_type,
                                    msg_messageId,
                                    msg_endToEndId,
                                    msg_returnId,
                                    msg_originalEndToEndId,
                                    msg_originalInstructionId,
                                    msg_originalMessageId,
                                    msg_originalResourceId,
                                    validationStatus,
                                    inf_dataGmt,
                                    inf_dataUtc,
                                    msg_originISPB,
                                    msg_destinyISPB,
                                    msg_scope,
                                    size > 1
                                );
                                if(temp != null)
                                    newIds.add(new InsertResult(msg_originalInstructionId, temp, msg_originalResourceId, msg_originalMessageId, msg_originalInstructionId, msg_originalEndToEndId));
                            }
                        }
                        break;
                    
                    case "pacs004":
                        if(message != null){
                            size = message.getJSONArray("returns").length();
                            for(int i = 0; i < size; i++){
                                JSONObject data = new JSONObject(message.getJSONArray("returns").get(i).toString());
                                msg_endToEndId            = null;
                                msg_returnId              = Library.getStringFromJson(data, "returnId");
                                msg_originalEndToEndId    = Library.getStringFromJson(data, "endToEndId");
                                msg_originalInstructionId = null;
                                msg_originalMessageId     = null;
                                msg_originalResourceId    = null;
                                temp = this.saveReceivedMessage(
                                    xml,
                                    http_code,
                                    http_head,
                                    http_body,
                                    http_time,
                                    piResourceId,
                                    msg_version,
                                    msg_code,
                                    msg_type,
                                    msg_messageId,
                                    msg_endToEndId,
                                    msg_returnId,
                                    msg_originalEndToEndId,
                                    msg_originalInstructionId,
                                    msg_originalMessageId,
                                    msg_originalResourceId,
                                    validationStatus,
                                    inf_dataGmt,
                                    inf_dataUtc,
                                    msg_originISPB,
                                    msg_destinyISPB,
                                    msg_scope,
                                    size > 1
                                );
                                newIds.add(new InsertResult(msg_returnId, temp, msg_originalResourceId, msg_originalMessageId, msg_originalInstructionId, msg_originalEndToEndId));
                            }
                        }
                        break;
                        
                    case "pain013":
                        if(message != null){
                            JSONObject data = new JSONObject(message.getJSONObject("paymentData").toString());
                            msg_endToEndId            = Library.getStringFromJson(data, "endToEndId");
                            msg_returnId              = null;
                            msg_originalEndToEndId    = null;
                            msg_originalInstructionId = null;
                            msg_originalMessageId     = null;
                            msg_originalResourceId    = null;
                            temp = this.saveReceivedMessage(
                                xml,
                                http_code,
                                http_head,
                                http_body,
                                http_time,
                                piResourceId,
                                msg_version,
                                msg_code,
                                msg_type,
                                msg_messageId,
                                msg_endToEndId,
                                msg_returnId,
                                msg_originalEndToEndId,
                                msg_originalInstructionId,
                                msg_originalMessageId,
                                msg_originalResourceId,
                                validationStatus,
                                inf_dataGmt,
                                inf_dataUtc,
                                msg_originISPB,
                                msg_destinyISPB,
                                msg_scope,
                                false
                            );
                            newIds.add(new InsertResult(msg_endToEndId, temp, msg_originalResourceId, msg_originalMessageId, msg_originalInstructionId, msg_originalEndToEndId));
                        }
                        break;
                        
                    case "pain014":
                        if(message != null){
                            msg_endToEndId            = null;
                            msg_returnId              = null;
                            msg_originalEndToEndId    = Library.getStringFromJson(message, "originalEndToEndId");
                            msg_originalInstructionId = null;
                            msg_originalMessageId     = Library.getStringFromJson(message, "originalMessageId");
                            msg_originalResourceId    = null;
                            temp = this.saveReceivedMessage(
                                xml,
                                http_code,
                                http_head,
                                http_body,
                                http_time,
                                piResourceId,
                                msg_version,
                                msg_code,
                                msg_type,
                                msg_messageId,
                                msg_endToEndId,
                                msg_returnId,
                                msg_originalEndToEndId,
                                msg_originalInstructionId,
                                msg_originalMessageId,
                                msg_originalResourceId,
                                validationStatus,
                                inf_dataGmt,
                                inf_dataUtc,
                                msg_originISPB,
                                msg_destinyISPB,
                                msg_scope,
                                false
                            );
                            newIds.add(new InsertResult(msg_originalEndToEndId, temp, msg_originalResourceId, msg_originalMessageId, msg_originalInstructionId, msg_originalEndToEndId));
                        }
                        break;
                        
                    case "camt052":
                        if(message != null){
                            size = message.getJSONArray("originalMessageId").length();
                            for(int i = 0; i < size; i++){
                                msg_endToEndId            = null;
                                msg_returnId              = null;
                                msg_originalEndToEndId    = null;
                                msg_originalInstructionId = null;
                                msg_originalMessageId     = message.has("originalMessageId") && message.getJSONArray("originalMessageId").length() > 0 ? message.getJSONArray("originalMessageId").get(i).toString() : null;
                                msg_originalResourceId    = null;
                                temp = this.saveReceivedMessage(
                                    xml,
                                    http_code,
                                    http_head,
                                    http_body,
                                    http_time,
                                    piResourceId,
                                    msg_version,
                                    msg_code,
                                    msg_type,
                                    msg_messageId,
                                    msg_endToEndId,
                                    msg_returnId,
                                    msg_originalEndToEndId,
                                    msg_originalInstructionId,
                                    msg_originalMessageId,
                                    msg_originalResourceId,
                                    validationStatus,
                                    inf_dataGmt,
                                    inf_dataUtc,
                                    msg_originISPB,
                                    msg_destinyISPB,
                                    msg_scope,
                                    size > 1
                                );
                                if(temp != null)
                                    newIds.add(new InsertResult(msg_originalMessageId, temp, msg_originalResourceId, msg_originalMessageId, msg_originalInstructionId, msg_originalEndToEndId));
                            }
                        }
                        break;
                        
                    case "camt053":
                        if(message != null){
                            size = message.getJSONArray("originalMessageId").length();
                            for(int i = 0; i < size; i++){
                                msg_endToEndId            = null;
                                msg_returnId              = null;
                                msg_originalEndToEndId    = null;
                                msg_originalInstructionId = null;
                                msg_originalMessageId     = message.has("originalMessageId") && message.getJSONArray("originalMessageId").length() > 0 && !message.getJSONArray("originalMessageId").get(i).toString().equals("00000000000000000000000000000000") ?  message.getJSONArray("originalMessageId").get(i).toString() : null;
                                msg_originalResourceId    = null;
                                temp = this.saveReceivedMessage(
                                    xml,
                                    http_code,
                                    http_head,
                                    http_body,
                                    http_time,
                                    piResourceId,
                                    msg_version,
                                    msg_code,
                                    msg_type,
                                    msg_messageId,
                                    msg_endToEndId,
                                    msg_returnId,
                                    msg_originalEndToEndId,
                                    msg_originalInstructionId,
                                    msg_originalMessageId,
                                    msg_originalResourceId,
                                    validationStatus,
                                    inf_dataGmt,
                                    inf_dataUtc,
                                    msg_originISPB,
                                    msg_destinyISPB,
                                    msg_scope,
                                    size > 1
                                );
                                newIds.add(new InsertResult(msg_originalMessageId, temp, msg_originalResourceId, msg_originalMessageId, msg_originalInstructionId, msg_originalEndToEndId));
                            }
                        }
                        break;
                        
                    case "camt054":
                        if(message != null){
                            size = message.getJSONArray("originalMessageId").length();
                            for(int i = 0; i < size; i++){
                                msg_endToEndId            = null;
                                msg_returnId              = null;
                                msg_originalEndToEndId    = null;
                                msg_originalInstructionId = null;
                                msg_originalMessageId     = message.has("originalMessageId") && message.getJSONArray("originalMessageId").length() > 0 ? message.getJSONArray("originalMessageId").get(i).toString() : null;
                                msg_originalResourceId    = null;
                                temp = this.saveReceivedMessage(
                                    xml,
                                    http_code,
                                    http_head,
                                    http_body,
                                    http_time,
                                    piResourceId,
                                    msg_version,
                                    msg_code,
                                    msg_type,
                                    msg_messageId,
                                    msg_endToEndId,
                                    msg_returnId,
                                    msg_originalEndToEndId,
                                    msg_originalInstructionId,
                                    msg_originalMessageId,
                                    msg_originalResourceId,
                                    validationStatus,
                                    inf_dataGmt,
                                    inf_dataUtc,
                                    msg_originISPB,
                                    msg_destinyISPB,
                                    msg_scope,
                                    size > 1
                                );
                                newIds.add(new InsertResult(msg_originalMessageId, temp, msg_originalResourceId, msg_originalMessageId, msg_originalInstructionId, msg_originalEndToEndId));
                            }
                        }
                        break;
                        
                    case "reda016":
                        msg_endToEndId            = null;
                        msg_returnId              = null;
                        msg_originalEndToEndId    = null;
                        msg_originalInstructionId = null;
                        msg_originalMessageId     = Library.getStringFromJson(message, "originalMessageId");
                        msg_originalResourceId    = null;
                        temp = this.saveReceivedMessage(
                            xml,
                            http_code,
                            http_head,
                            http_body,
                            http_time,
                            piResourceId,
                            msg_version,
                            msg_code,
                            msg_type,
                            msg_messageId,
                            msg_endToEndId,
                            msg_returnId,
                            msg_originalEndToEndId,
                            msg_originalInstructionId,
                            msg_originalMessageId,
                            msg_originalResourceId,
                            validationStatus,
                            inf_dataGmt,
                            inf_dataUtc,
                            msg_originISPB,
                            msg_destinyISPB,
                            msg_scope,
                            false
                        );
                        newIds.add(new InsertResult(msg_originalMessageId, temp, msg_originalResourceId, msg_originalMessageId, msg_originalInstructionId, msg_originalEndToEndId));
                        break;
                        
                    default:
                        msg_endToEndId            = message.has("endToEndId") ? message.get("endToEndId").toString() : null;
                        msg_returnId              = message.has("returnId") ? message.get("returnId").toString() : null;
                        msg_originalEndToEndId    = message.has("originalEndToEndId") ? message.get("originalEndToEndId").toString() : null;
                        msg_originalInstructionId = message.has("originalInstructionId") ? message.get("originalInstructionId").toString() : null;
                        msg_originalMessageId     = message.has("originalMessageId") ? message.get("originalMessageId").toString() : null;
                        msg_originalResourceId    = message.has("originalResourceId") ? message.get("originalResourceId").toString() : null;
                        temp = this.saveReceivedMessage(
                            xml,
                            http_code,
                            http_head,
                            http_body,
                            http_time,
                            piResourceId,
                            msg_version,
                            msg_code,
                            msg_type,
                            msg_messageId,
                            msg_endToEndId,
                            msg_returnId,
                            msg_originalEndToEndId,
                            msg_originalInstructionId,
                            msg_originalMessageId,
                            msg_originalResourceId,
                            validationStatus,
                            inf_dataGmt,
                            inf_dataUtc,
                            msg_originISPB,
                            msg_destinyISPB,
                            msg_scope,
                            false
                        );
                        newIds.add(new InsertResult(msg_originalResourceId, temp, msg_originalResourceId, msg_originalMessageId, msg_originalInstructionId, msg_originalEndToEndId));
                }

            }

            return newIds;

        } catch (Exception ex) {
            Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), "Erro ao salvar mensagem no MongoDB");
            Logger.writeException(ex);

        }
        return newIds;
    }

    public BsonValue saveReceivedMessage(String xml, int http_code, String http_head, String http_body, int http_time, String piResourceId, String msg_version, String msg_code, String msg_type, String msg_messageId, String msg_endToEndId, String msg_returnId, String msg_originalEndToEndId, String msg_originalInstructionId, String msg_originalMessageId, String msg_originalResourceId, String validationStatus, String inf_dataGmt, String inf_dataUtc, String msg_originISPB, String msg_destinyISPB, String msg_scope, boolean multiple){

        //Message
        Document messageBd = new Document()
            .append("aberta", xml);

        //Http info
        Document http = new Document()
            .append("code", http_code)
            .append("head", http_head)
            .append("body", http_body)
            .append("time", http_time);

        //Message data
        Document messageData = new Document()
            .append("multipla", multiple)
            .append("piResourceId", piResourceId)
            .append("versao", msg_version)
            .append("codigo", msg_code)
            .append("tipo", msg_type)
            .append("messageId", msg_messageId)
            .append("endToEndId", msg_endToEndId)
            .append("returnId", msg_returnId)
            .append("originalEndToEndId", msg_originalEndToEndId)
            .append("originalInstructionId", msg_originalInstructionId)
            .append("originalMessageId", msg_originalMessageId)
            .append("originalResourceId", msg_originalResourceId);

        //Document
        Document doc = new Document()
            .append("status", validationStatus)
            .append("data_gmt", inf_dataGmt)
            .append("data_utc", inf_dataUtc)
            .append("ispb_origem", msg_originISPB)
            .append("ispb_destino", msg_destinyISPB)
            .append("escopo", msg_scope)
            .append("dados_mensagem", messageData)
            .append("http", http)
            .append("mensagem", messageBd);

        InsertOneResult result = COLLECTION_RECEIVER.insertOne(doc);
        System.out.println("Last insert ID (Received "+msg_code+"): "+result.getInsertedId());
        return result.getInsertedId();

    }

    public void linkMessages(UUID threadId, String idReceived, String codeReceived, String originalResourceIds, String originalMessageIds, String originalInstructionIds, String originalEndToEndIds) {

        this.linkMessagesOnSender(threadId, idReceived, codeReceived, originalResourceIds, originalMessageIds, originalInstructionIds, originalEndToEndIds);
        this.linkMessagesOnReceiver(threadId, idReceived, codeReceived, originalResourceIds, originalMessageIds, originalInstructionIds, originalEndToEndIds);

    }

    public void linkMessagesById(BsonValue idSender, String typeSender, BsonValue idReceiver, String typeReceiver){

        //Update sender
        Bson filter = eq("_id", new ObjectId(idSender.asObjectId().getValue().toString()));
        Bson projection = include("dados_mensagem");
        FindIterable<Document> result = this.COLLECTION_SENDER.find(filter).sort(Library.getDefaultSort(-1)).projection(projection).limit(1).hint(Library.getIndex(1));
        result.forEach(iterable -> {
            JSONObject msg = new JSONObject(iterable.toJson());
            JSONObject messageData = msg.getJSONObject("dados_mensagem");
            ArrayList<BasicDBObject> linksSender = new ArrayList<>();

            if(messageData.has("vinculo") && messageData.get("vinculo") != null){
                JSONArray currentLinks;
                try{
                    currentLinks = messageData.getJSONArray("vinculo");
                } catch (JSONException e){
                    currentLinks = new JSONArray();
                }

                int size = currentLinks.length();
                for(int i = 0; i < size; i++){
                    JSONObject link = new JSONObject(currentLinks.get(i).toString());
                    linksSender.add(new BasicDBObject()
                        .append("id", link.getString("id"))
                        .append("tipo", link.getString("tipo"))
                    );
                }
            }

            saveMessagesLink(
                null,
                typeReceiver,
                idReceiver.asObjectId().getValue().toString(),
                this.getLinksFromReceived(idReceiver.asObjectId().getValue().toString()),
                typeSender,
                idSender.asObjectId().getValue().toString(),
                linksSender,
                false
            );

        });

    }

    public BsonValue saveWHLog(String endpoint, String idReceivedMessage, String request, String response, String requestId, String requestDate, String requestHeader, String responseDate, String responseHeader, int attempt, String reportType){

        try{

            BasicDBObject main = new BasicDBObject();
            main.append("tipo", reportType);
            main.append("request_id", requestId);
            main.append("endpoint", endpoint);
            main.append("data", Library.getDate(true, true, true, false, "BR"));
            main.append("tentativa", attempt);
            main.append("mensagem_recebida", idReceivedMessage);
            main.append("mensagem_enviada", null);

            BasicDBObject req = new BasicDBObject();
            req.append("data", requestDate);
            req.append("head", requestHeader);
            req.append("body", request);

            BasicDBObject resp = new BasicDBObject();
            resp.append("data", responseDate);
            resp.append("head", responseHeader);
            resp.append("body", response);

            main.append("http", new BasicDBObject().append("request", req).append("response", resp));
            InsertOneResult result = COLLECTION_WEBHOOK_LOG.insertOne(new Document(main));
            return result.getInsertedId();

       } catch(Exception ex){

            Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), "Não foi possível salvar o log do webhook no MongoDB: "+response);
            Logger.writeException(ex);
            return null;

       }

    }

    public void updateWHLog(BsonValue logId, BsonValue sendedMessageId){

        BasicDBObject updateDocument = new BasicDBObject();
        updateDocument.put("mensagem_enviada", sendedMessageId.asObjectId().getValue().toString());

        BasicDBObject updateObject = new BasicDBObject();
        updateObject.put("$set", updateDocument);

        BasicDBObject query = new BasicDBObject();
        query.put("_id", new ObjectId(logId.asObjectId().getValue().toString()));

        UpdateResult updtR = this.COLLECTION_WEBHOOK_LOG.updateOne(query, updateObject);
        if(updtR.getModifiedCount() > 0)
            Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_INFO(), "Mensagem enviada "+sendedMessageId+" vinculada à chamada "+logId);

    }
    
    public void setWHLogResponseId(BsonValue logId, String responseId) {
        
        BasicDBObject updateDocument = new BasicDBObject();
        updateDocument.put("id_resposta", responseId);

        BasicDBObject updateObject = new BasicDBObject();
        updateObject.put("$set", updateDocument);

        BasicDBObject query = new BasicDBObject();
        query.put("_id", new ObjectId(logId.asObjectId().getValue().toString()));

        UpdateResult updtR = this.COLLECTION_WEBHOOK_LOG.updateOne(query, updateObject);
        if(updtR.getModifiedCount() > 0)
            Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_INFO(), "Chamada do webhook "+logId+" atualizado com o id de resposta "+responseId);

        
    }

    public BsonValue savePAIN014(String xml, byte[] signedByteXml, String signedXml, String messageId, String instructionId, String endToEndId) {

        BasicDBObject main = new BasicDBObject();
        main.append("id_cliente", Integer.parseInt(Globals.getMONGO_ID_USER()));

        BasicDBObject message = new BasicDBObject();
        message.append("data_gmt", Library.getDate(true, true, true, false, "BR"));
        message.append("data_utc", Library.getDate(true, true, true, false, "UTC"));
        message.append("dados", xml);

        BasicDBObject signedMessage = new BasicDBObject();
        signedMessage.append("data_gmt", Library.getDate(true, true, true, false, "BR"));
        signedMessage.append("data_utc", Library.getDate(true, true, true, false, "UTC"));
        signedMessage.append("dados", signedXml);

        BasicDBObject messageData = new BasicDBObject();
        messageData.append("status", "PROCESSANDO");
        messageData.append("escopo", "SPI");
        messageData.append("tipo", Library.getMessageTypeLabel("pain014"));
        messageData.append("codigo", "pain014");

        BasicDBObject msgProps = new BasicDBObject()
            .append("msgId", messageId)
            .append("originalInstructionId", instructionId)
            .append("endToEnd", endToEndId);
        messageData.append("propriedades", msgProps);

        BasicDBObject http = new BasicDBObject();
        http.append("data_gmt", Library.getDate(true, true, true, false, "BR"));
        http.append("data_utc", Library.getDate(true, true, true, false, "UTC"));
        http.append("method", "POST");
        http.append("host", Globals.getPIX_HOST());
        http.append("strKeyId", Globals.getHSM_PIC_KEY());
        http.append("strCertId", Globals.getHSM_PIC_CER());
        http.append("strPixCertChainId", Globals.getHSM_CHAIN_PIC());
        http.append("strUrl", Globals.getICOM_BASE_ENDPOINT_SENDER());
        http.append("baRequestData", signedByteXml);
        http.append("hsm_i", "10.204.125.100");
        http.append("hsm_u", Globals.getHSM_USER());
        http.append("hsm_p", Globals.getHSM_PASSWORD());

        main.append("mensagem_montada", message);
        main.append("mensagem_assinada", signedMessage);
        main.append("dados_mensagem", messageData);
        main.append("http", new BasicDBObject().append("send_data", http));

        InsertOneResult result = COLLECTION_SENDER.insertOne(new Document(main));
        System.out.println("Last insert ID (New PAIN014): "+result.getInsertedId());
        return result.getInsertedId();

    }

    public BsonValue savePACS002(String xml, byte[] signedByteXml, String signedXml, String messageId, JSONArray instructionId, JSONArray endToEndId) {

        try {
            
            BasicDBObject main = new BasicDBObject();
            main.append("id_cliente", Integer.parseInt(Globals.getMONGO_ID_USER()));

            BasicDBObject message = new BasicDBObject();
            message.append("data_gmt", Library.getDate(true, true, true, false, "BR"));
            message.append("data_utc", Library.getDate(true, true, true, false, "UTC"));
            message.append("dados", xml);

            BasicDBObject signedMessage = new BasicDBObject();
            signedMessage.append("data_gmt", Library.getDate(true, true, true, false, "BR"));
            signedMessage.append("data_utc", Library.getDate(true, true, true, false, "UTC"));
            signedMessage.append("dados", signedXml);

            BasicDBObject messageData = new BasicDBObject();
            messageData.append("status", "PROCESSANDO");
            messageData.append("escopo", "SPI");
            messageData.append("tipo", Library.getMessageTypeLabel("pacs002"));
            messageData.append("codigo", "pacs002");

            BasicDBObject msgProps = new BasicDBObject()
                .append("msgId", messageId)
                .append("originalInstructionId", instructionId.length() > 1 ? instructionId : (String) instructionId.get(0))
                .append("endToEnd", endToEndId.length() > 1 ? endToEndId : (String) endToEndId.get(0));
            messageData.append("propriedades", msgProps);

            BasicDBObject http = new BasicDBObject();
            http.append("data_gmt", Library.getDate(true, true, true, false, "BR"));
            http.append("data_utc", Library.getDate(true, true, true, false, "UTC"));
            http.append("method", "POST");
            http.append("host", Globals.getPIX_HOST());
            http.append("strKeyId", Globals.getHSM_PIC_KEY());
            http.append("strCertId", Globals.getHSM_PIC_CER());
            http.append("strPixCertChainId", Globals.getHSM_CHAIN_PIC());
            http.append("strUrl", Globals.getICOM_BASE_ENDPOINT_SENDER());
            http.append("baRequestData", signedByteXml);
            http.append("hsm_i", "10.204.125.100");
            http.append("hsm_u", Globals.getHSM_USER());
            http.append("hsm_p", Globals.getHSM_PASSWORD());

            main.append("mensagem_montada", message);
            main.append("mensagem_assinada", signedMessage);
            main.append("dados_mensagem", messageData);
            main.append("http", new BasicDBObject().append("send_data", http));

            InsertOneResult result = COLLECTION_SENDER.insertOne(new Document(main));
            System.out.println("Last insert ID (New PACS002): "+result.getInsertedId());
            return result.getInsertedId();
            
        } catch(Exception ex) {
            
            Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), "Não foi possível salvar a mensagem pacs002 com o messageId "+messageId);
            Logger.writeException(ex);
            return null;
            
        }
        

    }

    public void saveADMI002Return(BsonValue mongoId, String status, String errorReason){

        BasicDBObject updateObject = new BasicDBObject();
        BasicDBObject updateDocument = new BasicDBObject();
        updateDocument.put("dados_mensagem.status", status);
        updateDocument.put("dados_mensagem.error.reason", errorReason);
        updateObject.put("$set", updateDocument);

        Bson filter = elemMatch("dados_mensagem.vinculo", and(eq("id", mongoId.asObjectId().getValue().toString()), eq("tipo", "admi002")));
        UpdateResult result = this.COLLECTION_SENDER.updateOne(filter, updateObject);
        
        if(result.getModifiedCount() > 0)
            Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_INFO(), "Status do document "+mongoId+" atualizado para "+status);
        else
            Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_WARNING(), "Não foi possível atualizar o status do document "+mongoId+" para "+status);

    }

    public void saveCAMT052Return(String originalMessageId, String status){

        BasicDBObject updateObject = new BasicDBObject();
        BasicDBObject updateDocument = new BasicDBObject();
        updateDocument.put("dados_mensagem.status", status);
        updateObject.put("$set", updateDocument);

        Bson filter = and(eq("dados_mensagem.propriedades.msgId", originalMessageId), eq("dados_mensagem.codigo", "camt.060"));
        UpdateResult result = this.COLLECTION_SENDER.updateOne(filter, updateObject);
        
        if(result.getModifiedCount() > 0)
            Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_INFO(), "Status do document com originalMessageId "+originalMessageId+" na sender atualizado para "+status);
        else
            Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_WARNING(), "Não foi possível atualizar o status do document com originalMessageId "+originalMessageId+" para "+status+" na sender");

    }

    public void saveCAMT053Return(String originalMessageId, String status){

        BasicDBObject updateObject = new BasicDBObject();
        BasicDBObject updateDocument = new BasicDBObject();
        updateDocument.put("dados_mensagem.status", status);
        updateObject.put("$set", updateDocument);

        Bson filter = and(eq("dados_mensagem.propriedades.msgId", originalMessageId), eq("dados_mensagem.codigo", "camt.060"));
        UpdateResult result = this.COLLECTION_SENDER.updateOne(filter, updateObject);
        
        if(result.getModifiedCount() > 0)
            Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_INFO(), "Status do document com originalMessageId "+originalMessageId+" na sender atualizado para "+status);
        else
            Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_WARNING(), "Não foi possível atualizar o status do document com originalMessageId "+originalMessageId+" para "+status+" na sender");

    }

    public void saveCAMT054Return(String originalMessageId, String status){

        BasicDBObject updateObject = new BasicDBObject();
        BasicDBObject updateDocument = new BasicDBObject();
        updateDocument.put("dados_mensagem.status", status);
        updateObject.put("$set", updateDocument);

        Bson filter = and(eq("dados_mensagem.propriedades.msgId", originalMessageId), eq("dados_mensagem.codigo", "camt.060"));
        UpdateResult result = this.COLLECTION_SENDER.updateOne(filter, updateObject);
        
        if(result.getModifiedCount() > 0)
            Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_INFO(), "Status do document com originalMessageId "+originalMessageId+" na sender atualizado para "+status);
        else
            Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_WARNING(), "Não foi possível atualizar o status do document com originalMessageId "+originalMessageId+" para "+status+" na sender");

    }

    public void saveREDA016Return(String originalMessageId, String status, String errorCode){

        BasicDBObject updateObject = new BasicDBObject();
        BasicDBObject updateDocument = new BasicDBObject();
        updateDocument.put("dados_mensagem.status", status);

        if(errorCode != null)
            updateDocument.put("dados_mensagem.erro.code", errorCode);

        updateObject.put("$set", updateDocument);

        Bson filter = and(eq("dados_mensagem.propriedades.msgId", originalMessageId), or(eq("dados_mensagem.codigo", "reda.014"), eq("dados_mensagem.codigo", "reda.022"), eq("dados_mensagem.codigo", "reda.031")));
        UpdateResult result = this.COLLECTION_SENDER.updateOne(filter, updateObject);
        
        if(result.getModifiedCount() > 0)
            Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_INFO(), "Status do document com originalMessageId "+originalMessageId+" na sender atualizado para "+status);
        else
            Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_WARNING(), "Não foi possível atualizar o status do document com originalMessageId "+originalMessageId+" para "+status+" na sender");

    }

    public void updatePAIN013Status(String originalMessageId, String status) {

        BasicDBObject updateDocument = new BasicDBObject();
        updateDocument.put("dados_mensagem.status", status);

        //Update on sender
        BasicDBObject updateObject = new BasicDBObject();
        updateObject.put("$set", updateDocument);

        BasicDBObject query = new BasicDBObject();
        query.put("dados_mensagem.propriedades.msgId", originalMessageId);
        query.put("dados_mensagem.codigo", "pain.013");

        UpdateResult updtR = this.COLLECTION_SENDER.updateOne(query, updateObject);
        if(updtR.getModifiedCount() > 0)
            Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_INFO(), "Status da mensagem (Sender) "+originalMessageId+" atualizado para "+status);

        //Update on receiver
        BasicDBObject updateObject2 = new BasicDBObject();
        updateObject2.put("$set", updateDocument);

        BasicDBObject query2 = new BasicDBObject();
        query2.put("dados_mensagem.messageId", originalMessageId);
        query2.put("dados_mensagem.codigo", "pain013");

        UpdateResult updtR2 = this.COLLECTION_RECEIVER.updateOne(query2, updateObject2);
        if(updtR2.getModifiedCount() > 0)
            Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_INFO(), "Status da mensagem (Receiver) "+originalMessageId+" atualizado para "+status);

    }

    public void updateStatusBasedOnPacs002(String originalInstructionId, String status, String errorCode, String errorDescription){
        
        String originalCodeSender = originalInstructionId.charAt(0) == 'E' ? "pacs.008" : "pacs.004";
        String referenceIndexSender = originalInstructionId.charAt(0) == 'E' ? "endToEnd" : "returnId";
        String originalCodeReceiver = originalInstructionId.charAt(0) == 'E' ? "pacs008" : "pacs004";
        String referenceIndexReceiver = originalInstructionId.charAt(0) == 'E' ? "endToEndId" : "returnId";
        
        // Create update object
        BasicDBObject updateDocument = new BasicDBObject();
        updateDocument.put("dados_mensagem.status", status);
        if(errorCode != null)
            updateDocument.put("dados_mensagem.error.code", errorCode);
        if(errorDescription != null)
            updateDocument.put("dados_mensagem.error.description", errorDescription);

        BasicDBObject updateObject = new BasicDBObject();
        updateObject.put("$set", updateDocument);
        
        //Update on sender
        BasicDBObject querySen = new BasicDBObject();
        querySen.put("dados_mensagem.propriedades."+referenceIndexSender, originalInstructionId);
        querySen.put("dados_mensagem.codigo", originalCodeSender);

        UpdateResult updtSen = this.COLLECTION_SENDER.updateOne(querySen, updateObject);
        if(updtSen.getModifiedCount() > 0)
            Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_INFO(), "Status da mensagem (Sender) "+originalInstructionId+" atualizado para "+status);
        
        //Update on receiver
        BasicDBObject queryRec = new BasicDBObject();
        queryRec.put("dados_mensagem."+referenceIndexReceiver, originalInstructionId);
        queryRec.put("dados_mensagem.codigo", originalCodeReceiver);

        UpdateResult updtRec = this.COLLECTION_RECEIVER.updateOne(queryRec, updateObject);
        if(updtRec.getModifiedCount() > 0)
            Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_INFO(), "Status da mensagem (Receiver) "+originalInstructionId+" atualizado para "+status);
        
    }

    public boolean reportedMessage(String messageId){
        
        if(messageId == null)
            return true;
        
        Bson filter = and(Arrays.asList(
                eq("message_id", messageId),
                eq("tipo", "REPORT"), 
                and(
                    exists("id_resposta", true),
                    ne("id_resposta", new BsonNull())
                )
        ));
        
        FindIterable<Document> result = this.COLLECTION_WEBHOOK_LOG.find(filter).sort(Library.getDefaultSort(-1)).hint(Library.getIndex(1)).limit(1);
        for(Document doc : result){
            
            JSONObject report = new JSONObject(doc.toJson());
            String messageIdReport = Library.getStringFromJson(report, "message_id");
            return messageId.equals(messageIdReport);
            
        }
        
        return false;
        
    }
    
    public ArrayList<String> getMessageLinks(BsonValue mongoId){
        
        try {
            
            ArrayList<String> linksIds = new ArrayList<>();
            
            Bson filter = eq("_id", new ObjectId(mongoId.asObjectId().getValue().toString()));
            Bson projection = include("dados_mensagem.vinculo");
            FindIterable<Document> result = this.COLLECTION_RECEIVER
                .find(filter)
                .sort(Library.getDefaultSort(-1))
                .projection(projection)
                .hint(Library.getIndex(1))
                .limit(1);

            for(Document doc : result){

                JSONObject messageData = new JSONObject(doc.toJson());
                JSONArray links = messageData.getJSONObject("dados_mensagem").getJSONArray("vinculo");
                int size = links.length();
                for(int i = 0; i < size; i++){
                    
                    JSONObject currentLink = new JSONObject(links.get(i).toString());
                    if(currentLink.has("id"))
                        linksIds.add(currentLink.getString("id"));
                    
                }
                
                return this.getUsersOfSendedMessages(linksIds);
                
            }
            
        } catch(Exception ex) {
            
            Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), "Não foi possível identificar os links da mensagem com mongo id "+mongoId);
            Logger.writeException(ex);
            
        }
        
        return new ArrayList<>();
        
    }
    
    public boolean insereFilaProcessamento(String messageIdDB){
        
        try {
            
            Document doc = new Document()
            .append("messageIdDB", messageIdDB);

            InsertOneResult result = COLLECTION_FILA_SPI.insertOne(doc);
            return result.getInsertedId() != null;
            
        } catch (Exception ex) {
            
            Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), "Não foi possível marcar a mensagem para processamento com mongoID "+messageIdDB);
            Logger.writeException(ex);
            return false;
            
        }
        
    }
    
    private void linkMessagesOnSender(UUID threadId, String idReceived, String codeReceived, String originalResourceIds, String originalMessageIds, String originalInstructionIds, String originalEndToEndIds) {
        
        Bson filter = and(Arrays.asList(
            eq("dados_mensagem.escopo", "SPI"),
            ne("_id", new ObjectId(idReceived)),
            or(Arrays.asList(
                eq("dados_mensagem.PI-ResourceId", originalResourceIds),
                eq("dados_mensagem.propriedades.msgId", originalMessageIds),
                eq("dados_mensagem.propriedades.endToEnd", originalInstructionIds),
                eq("dados_mensagem.propriedades.returnId", originalInstructionIds),
                eq("dados_mensagem.propriedades.endToEnd", originalEndToEndIds)
            )))
        );
        
        Bson projection = include(
            "_id",
            "dados_mensagem"
        );
        
        FindIterable<Document> result = this.COLLECTION_SENDER
            .find(filter)
            .sort(Library.getDefaultSort(-1))
            .limit(1)
            .projection(projection);
        
        result.forEach(iterable -> {

            JSONObject msg = new JSONObject(iterable.toJson());
            JSONObject messageData = msg.getJSONObject("dados_mensagem");

            String codeSender = messageData.has("codigo") ? messageData.getString("codigo") : "desconhecido";
            String idSender = msg.getJSONObject("_id").getString("$oid");
            ArrayList<BasicDBObject> linksSender = new ArrayList<>();

            if(messageData.has("vinculo") && messageData.get("vinculo") != null){

                JSONArray currentLinks;
                try{
                    currentLinks = messageData.getJSONArray("vinculo");
                } catch (JSONException e){
                    currentLinks = new JSONArray();
                }

                int size = currentLinks.length();
                for(int i = 0; i < size; i++){
                    JSONObject link = new JSONObject(currentLinks.get(i).toString());
                    linksSender.add(new BasicDBObject()
                        .append("id", link.getString("id"))
                        .append("tipo", link.getString("tipo"))
                    );
                }
            }

            saveMessagesLink(
                threadId,
                codeReceived,
                idReceived,
                this.getLinksFromReceived(idReceived),
                codeSender,
                idSender,
                linksSender,
                false
            );

        });

    }

    private void linkMessagesOnReceiver(UUID threadId, String idReceived, String codeReceived, String originalResourceIds, String originalMessageIds, String originalInstructionIds, String originalEndToEndIds) {
        
        Bson filter = and(Arrays.asList(
            eq("dados_mensagem.escopo", "SPI"),
            ne("_id", new ObjectId(idReceived)),
            or(Arrays.asList(
                eq("dados_mensagem.piResourceId", originalResourceIds),
                eq("dados_mensagem.messageId", originalMessageIds),
                eq("dados_mensagem.endToEndId", originalInstructionIds),
                eq("dados_mensagem.returnId", originalInstructionIds),
                eq("dados_mensagem.endToEndId", originalEndToEndIds)
            )))
        );
        
        Bson projection = include(
            "_id",
            "dados_mensagem"
        );

        FindIterable<Document> result = this.COLLECTION_RECEIVER
            .find(filter)
            .sort(Library.getDefaultSort(-1))
            .limit(1)
            .projection(projection);
        
        result.forEach(iterable -> {
            
            JSONObject msg = new JSONObject(iterable.toJson());
            JSONObject messageData = msg.getJSONObject("dados_mensagem");

            String codeSender = messageData.has("codigo") ? messageData.getString("codigo") : "desconhecido";
            String idSender = msg.getJSONObject("_id").getString("$oid");
            ArrayList<BasicDBObject> linksSender = new ArrayList<>();

            if(messageData.has("vinculo") && messageData.get("vinculo") != null){
                JSONArray currentLinks;
                try{
                    currentLinks = messageData.getJSONArray("vinculo");
                } catch (JSONException e){
                    currentLinks = new JSONArray();
                }

                int size = currentLinks.length();
                for(int i = 0; i < size; i++){
                    JSONObject link = new JSONObject(currentLinks.get(i).toString());
                    linksSender.add(new BasicDBObject()
                        .append("id", link.getString("id"))
                        .append("tipo", link.getString("tipo"))
                    );
                }
            }

            saveMessagesLink(
                threadId,
                codeReceived,
                idReceived,
                this.getLinksFromReceived(idReceived),
                codeSender,
                idSender,
                linksSender,
                true);

        });

    }

    private void saveMessagesLink(UUID threadId, String codeReceiver, String idReceiver, ArrayList<BasicDBObject> linksReceiver, String codeSender, String idSender, ArrayList<BasicDBObject> linksSender, boolean linkBetweenReceivers) {

        //Update first document
        linksReceiver.add(new BasicDBObject()
            .append("id", idSender)
            .append("tipo", codeSender)
        );

        BasicDBObject updateObjectReceiver = new BasicDBObject();
        BasicDBObject updateDocumentReceiver = new BasicDBObject();
        updateDocumentReceiver.put("dados_mensagem.vinculo", linksReceiver);
        updateObjectReceiver.put("$set", updateDocumentReceiver);

        BasicDBObject queryReceiver = new BasicDBObject();
        queryReceiver.put("_id", new ObjectId(idReceiver));
        this.COLLECTION_RECEIVER.updateOne(queryReceiver, updateObjectReceiver);

        //Update second document
        linksSender.add(new BasicDBObject()
            .append("id", idReceiver)
            .append("tipo", codeReceiver)
        );

        BasicDBObject updateObjectSender = new BasicDBObject();
        BasicDBObject updateDocumentSender = new BasicDBObject();
        updateDocumentSender.put("dados_mensagem.vinculo", linksSender);
        updateObjectSender.put("$set", updateDocumentSender);

        BasicDBObject querySender = new BasicDBObject();
        querySender.put("_id", new ObjectId(idSender));

        if(linkBetweenReceivers){
            this.COLLECTION_RECEIVER.updateOne(querySender, updateObjectSender);
            Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_INFO(), "Thread "+threadId+" criou link entre "+idSender+" (Receiver) e "+idReceiver+" (Receiver)");
        } else {
            this.COLLECTION_SENDER.updateOne(querySender, updateObjectSender);
            Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_INFO(), "Thread "+threadId+" criou link entre "+idSender+" (Sender) e "+idReceiver+" (Receiver)");
        }

    }

    private ArrayList<BasicDBObject> getLinksFromReceived(String idReceived) {

        // Find links on receiver
        ArrayList<BasicDBObject> linksReceiver = new ArrayList<>();
        Bson filter = eq("_id", new ObjectId(idReceived));
        FindIterable<Document> result = this.COLLECTION_RECEIVER.find(filter).sort(Library.getDefaultSort(-1)).hint(Library.getIndex(1));
        result.forEach(iterable -> {

            JSONObject msg = new JSONObject(iterable.toJson());
            JSONObject messageData = msg.getJSONObject("dados_mensagem");

            if(messageData.has("vinculo") && messageData.get("vinculo") != null){
                JSONArray currentLinks = messageData.getJSONArray("vinculo");
                int size = currentLinks.length();
                for(int i = 0; i < size; i++){
                    JSONObject link = new JSONObject(currentLinks.get(i).toString());
                    linksReceiver.add(new BasicDBObject()
                        .append("id", link.getString("id"))
                        .append("tipo", link.getString("tipo"))
                    );
                }
            }

        });

        return linksReceiver;

    }

    private ArrayList<String> getUsersOfSendedMessages(ArrayList<String> messageIds){
        
        ArrayList<String> users = new ArrayList<>();
        List ids = new ArrayList<ObjectId>();
        for(String currentId : messageIds)
            ids.add(new ObjectId(currentId));
        
        if(!ids.isEmpty()){
            
            Bson filter = in("_id", ids);
            Bson projection = include("user_oauth2");
            FindIterable<Document> result = this.COLLECTION_SENDER.find(filter).sort(Library.getDefaultSort(-1)).projection(projection).hint(Library.getIndex(1));
            for(Document doc : result){
                JSONObject currentDoc = new JSONObject(doc.toJson());
                if(currentDoc.has("user_oauth2")){
                    if(!users.contains(currentDoc.getString("user_oauth2")))
                        users.add(currentDoc.getString("user_oauth2"));
                }
            }
            
        }
        
        return users;
        
    }
    
    @Override
    public String toString() {

        String client = "client: "+CLIENT.toString();
        String database = "database: "+DATABASE.getName();
        String col_receiver = "collection_receiver: "+COLLECTION_RECEIVER.getNamespace();
        String col_sender = "collection_sender: "+COLLECTION_SENDER.getNamespace();
        String col_webhook = "collection_webhook: "+COLLECTION_WEBHOOK_LOG.getNamespace();

        return "{\n\t"+client+",\n\t"+database+",\n\t"+col_receiver+",\n\t"+col_sender+",\n\t"+col_webhook+"\n}";

    }

}