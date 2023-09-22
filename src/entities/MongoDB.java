package entities;

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
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.ne;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import java.util.Arrays;
import static com.mongodb.client.model.Filters.exists;
import dead_letter_timeout_spi.Dead_letter_timeout_SPI;
import static dead_letter_timeout_spi.Dead_letter_timeout_SPI.FW;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.json.JSONObject;

public class MongoDB {

    private final MongoClient CLIENT;
    private final MongoDatabase DATABASE;
    private final MongoCollection<Document> COLLECTION_SENDER;
    private final MongoCollection<Document> COLLECTION_WEBHOOK_LOG;

    public MongoDB() {
        CLIENT = MongoClients.create(Dead_letter_timeout_SPI.QUERY_STRING);
        DATABASE = CLIENT.getDatabase(Dead_letter_timeout_SPI.DATABASE);
        COLLECTION_SENDER = DATABASE.getCollection("sender");
        COLLECTION_WEBHOOK_LOG = DATABASE.getCollection("wh_calls");
    }

    public boolean testConnection() {
        try {
            MongoDatabase database = CLIENT.getDatabase(Dead_letter_timeout_SPI.DATABASE);
            return database.listCollectionNames() != null;
        } catch (Exception ex) {
            Logger.writeException(ex);
            System.out.println("Erro ao testar conexão com o banco de dados: " + ex.getMessage());
            FW.write("Erro ao testar conexão com o banco de dados: " + ex.getMessage());
            return false;
        }
    }

    public MongoClient getCLIENT() {
        return CLIENT;
    }

    public MongoDatabase getDATABASE() {
        return DATABASE;
    }

    public MongoCollection<Document> getCOLLECTION_SENDER() {
        return COLLECTION_SENDER;
    }

    public MongoCollection<Document> getCOLLECTION_WEBHOOK_LOG() {
        return COLLECTION_WEBHOOK_LOG;
    }

    public BsonValue saveWHLog(String endpoint, String idReceivedMessage, String request, String response, String requestId, String requestDate, String requestHeader, String responseDate, String responseHeader, int attempt, String reportType) {
        try {
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
        } catch (Exception ex) {
            Logger.writeException(ex);
            System.out.println("Erro ao salvar log do webhook: " + ex.getMessage());
            FW.write("Erro ao salvar log do webhook: " + ex.getMessage());
            return null;
        }
    }

    public void updateWHLog(BsonValue logId, BsonValue sendedMessageId) {
        try {
            BasicDBObject updateDocument = new BasicDBObject();
            updateDocument.put("mensagem_enviada", sendedMessageId.asObjectId().getValue().toString());

            BasicDBObject updateObject = new BasicDBObject();
            updateObject.put("$set", updateDocument);

            BasicDBObject query = new BasicDBObject();
            query.put("_id", new ObjectId(logId.asObjectId().getValue().toString()));

            UpdateResult updtR = this.COLLECTION_WEBHOOK_LOG.updateOne(query, updateObject);
        } catch (Exception ex) {
            Logger.writeException(ex);
            System.out.println("Erro ao atualizar log do webhook: " + ex.getMessage());
            FW.write("Erro ao atualizar log do webhook: " + ex.getMessage());
        }
    }

    public void setWHLogResponseId(BsonValue logId, String responseId) {
        try {
            BasicDBObject updateDocument = new BasicDBObject();
            updateDocument.put("id_resposta", responseId);

            BasicDBObject updateObject = new BasicDBObject();
            updateObject.put("$set", updateDocument);

            BasicDBObject query = new BasicDBObject();
            query.put("_id", new ObjectId(logId.asObjectId().getValue().toString()));

            UpdateResult updtR = this.COLLECTION_WEBHOOK_LOG.updateOne(query, updateObject);
        } catch (Exception ex) {
            Logger.writeException(ex);
            System.out.println("Erro ao definir o ID de resposta no log do webhook: " + ex.getMessage());
            FW.write("Erro ao definir o ID de resposta no log do webhook: " + ex.getMessage());
        }
    }

    public boolean reportedMessage(String messageId) {
        try {
            Bson filter = and(Arrays.asList(
                    eq("message_id", messageId),
                    eq("tipo", "REPORT"), 
                    and(
                        exists("id_resposta", true),
                        ne("id_resposta", new BsonNull())
                    )
            ));

            FindIterable<Document> result = this.COLLECTION_WEBHOOK_LOG.find(filter).sort(Library.getDefaultSort(-1)).hint(Library.getIndex(1)).limit(1);
            for (Document doc : result) {
                JSONObject report = new JSONObject(doc.toJson());
                String messageIdReport = Library.getStringFromJson(report, "message_id");
                return messageId.equals(messageIdReport);
            }
        } catch (Exception ex) {
            Logger.writeException(ex);
            System.out.println("Erro ao verificar se a mensagem foi relatada: " + ex.getMessage());
            FW.write("Erro ao verificar se a mensagem foi relatada: " + ex.getMessage());
        }
        return false;
    }
}