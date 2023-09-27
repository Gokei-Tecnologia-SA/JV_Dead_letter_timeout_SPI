package entities;

import com.dead_letter_timeout_spi.common.Library;
import dead_letter_timeout_spi.Dead_letter_timeout_SPI;
import static dead_letter_timeout_spi.Dead_letter_timeout_SPI.FW;
import java.util.UUID;
import org.bson.BsonValue;
import org.w3c.dom.Document;

public class MessageReporter {

    private final int ATTEMPTS = 5;
    private final WebhookManager WEBHOOK_MANAGER = new WebhookManager();

    public String reportMessage(String oid, String xmlModificado, String messageId) {
        try {
            String messageCode = "admi.002";
            String messageVersion = "admi.002.spi.1.3";

            int currentAttempts = 1;
            String requestId = UUID.randomUUID().toString();
            String idempotencyKey = UUID.randomUUID().toString();

            while (currentAttempts <= ATTEMPTS) {
                WEBHOOK_MANAGER.setINSERTED_MESSAGE_ID(oid);
                WEBHOOK_MANAGER.setREQUEST_ID(requestId);
                String responseXML = WEBHOOK_MANAGER.sendReportRequest(messageCode, messageId, messageVersion, xmlModificado, currentAttempts, idempotencyKey);

                if (responseXML != null) {
                    BsonValue currentWebhookCallId = WEBHOOK_MANAGER.getID_WH_LOG();
                    Document xmlDoc = Library.xmlStringToDocument(responseXML);

                    if (xmlDoc != null) {
                        if (xmlDoc.getElementsByTagName("IDENTIFICATION").getLength() > 0) {
                            String responseId = xmlDoc.getElementsByTagName("IDENTIFICATION").item(0).getTextContent();
                            if (responseId != null) {
                                if (Library.isGUIDValid(responseId)) {
                                    Dead_letter_timeout_SPI.MDB.setWHLogResponseId(currentWebhookCallId, responseId);
                                    return responseId;
                                }
                            }
                        }
                    }
                }
                currentAttempts++;
                Library.sleep(5000);
            }
            return null; // Se não tiver sucesso após todas as tentativas, retorna null
        } catch (Exception ex) {
            FW.writeException(ex);
            return null;
        }
    }
}
