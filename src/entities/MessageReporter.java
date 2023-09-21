package entities;

import com.dead_letter_timeout_spi.common.Globals;
import com.dead_letter_timeout_spi.common.Library;
import com.dead_letter_timeout_spi.common.Logger;
import java.util.UUID;
import org.bson.BsonValue;
import org.json.JSONObject;
import org.w3c.dom.Document;

public class MessageReporter {
    
    private final int ATTEMPTS = Globals.getREPORT_ATTEMPTS();
    private final WebhookManager WEBHOOK_MANAGER = new WebhookManager();
    
    public void reportMessage(String oid, String xmlModificado){
       
        String messageCode = "admi.002";
        String messageId = "M0000000000000000000000000000000";
        String messageVersion = "admi.002.spi.1.3";
        
        int currentAttemps = 1;
        String requestId = UUID.randomUUID().toString();
        String idempotencyKey = UUID.randomUUID().toString();
        
        while(currentAttemps <= ATTEMPTS){
            
            WEBHOOK_MANAGER.setINSERTED_MESSAGE_ID(oid);
            WEBHOOK_MANAGER.setREQUEST_ID(requestId);
            String responseXML = WEBHOOK_MANAGER.sendReportRequest(messageCode, messageId, messageVersion, xmlModificado, currentAttemps, idempotencyKey);
            
            if(responseXML != null){
                
                BsonValue currentWebhookCallId = WEBHOOK_MANAGER.getID_WH_LOG();
                Document xmlDoc = Library.xmlStringToDocument(responseXML);

                if(xmlDoc != null){
                    if(xmlDoc.getElementsByTagName("IDENTIFICATION").getLength() > 0){
                        String responseId = xmlDoc.getElementsByTagName("IDENTIFICATION").item(0).getTextContent();
                        if(responseId != null){
                            if(Library.isGUIDValid(responseId)){                        
                                Globals.getMONGODB().setWHLogResponseId(currentWebhookCallId, responseId);
                                Logger.writeInfo(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_INFO(), "Report da mensagem com OID " + oid + " e requestId " + requestId + " teve sucesso. ResponseId: " + responseId);
                                return;
                            } else {
                                Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_WARNING(), "XML retornado na requisição com requestId "+requestId+" não contém um GUID válido.");
                            }
                        } else {
                            Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_WARNING(), "XML retornado na requisição com requestId "+requestId+" contém conteúdo inválido na tag IDENTIFICATION.");
                        }
                    } else {
                        Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_WARNING(), "XML retornado na requisição com requestId "+requestId+" não possui a tag IDENTIFICATION.");
                    }
                } else {
                    Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_WARNING(), "XML retornado na requisição com requestId "+requestId+" inválido.");
                }
            
            } else {
                Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_WARNING(), "Conteúdo retornado na requisição com requestId "+requestId+" inválido.");
            }

            currentAttemps++;
            Library.sleep(Globals.getREPORT_MAX_TIMETOUT());
            
        }
        
        Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), "Falha no relatório da mensagem com OID " + oid + " após " + ATTEMPTS + " tentativas.");
    }
}
