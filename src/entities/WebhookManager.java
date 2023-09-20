package entities;

import com.mongodb.BasicDBObject;
import com.mongodb.client.result.UpdateResult;
import dead_letter_spi.Dead_letter_timeout_SPI;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.bson.types.ObjectId;
import org.xml.sax.InputSource;
public class WebhookManager {
    
    private final int TIMEOUT = 5000;
    private final String TOKEN = "6A576E5A7234753777217A25432A462D4A614E645267556B58703273357638792F413F4428472B4B6250655368566D597133743677397A244326462948404D63";
    private final FileWriter FW = new FileWriter();
    
    public void sendRequest(String whId, int tentativaAtual, String reqEndpoint, String reqBody, String idempotencyKey, String messageId, String messageCode, String messageVersion) {
        
        HttpURLConnection connection = null;
        String reqHeader = null;
        String reqDate = null;
        StringBuilder respBody = new StringBuilder();
        String respHeader = null;
        String respDate = null;
        boolean validResponse = false;
        String responseId = null;
        
        try{

            URL url = new URL(reqEndpoint);
            connection = (HttpURLConnection) url.openConnection();            
            connection.setRequestMethod("POST");
            connection.setRequestProperty("content-type", "application/xml");
            connection.addRequestProperty("IdempotencyKey", idempotencyKey);
            connection.addRequestProperty("spi-message-id", messageId);
            connection.addRequestProperty("spi-message-code", messageCode);
            connection.addRequestProperty("spi-message-version", messageVersion);
            connection.addRequestProperty("current-attempt", String.valueOf(tentativaAtual+1));
            connection.setRequestProperty("Authorization", TOKEN);
            connection.setUseCaches(false);
            connection.setDoOutput(true);
            connection.setConnectTimeout(TIMEOUT);
            connection.setReadTimeout(TIMEOUT);       
            
            reqHeader = connection.getRequestProperties().toString();
            reqDate = getCurrentDate(true, true, true, "BR");
            
            FW.write("Reenvio da mensagem com mongoId "+whId+" usando o endpoint: "+reqEndpoint);
            FW.write("Reenvio da mensagem com mongoId "+whId+" usando o header: "+reqHeader);
            FW.write("Reenvio da mensagem com mongoId "+whId+" usando o body: "+reqBody);
            
            OutputStreamWriter wr = new OutputStreamWriter(connection.getOutputStream(), StandardCharsets.UTF_8);
            wr.write(reqBody);
            wr.close();

            InputStream is = connection.getInputStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(is));
            String line;
            
            while((line = rd.readLine()) != null){
                respBody.append(line);
            }
            
            respDate = getCurrentDate(true, true, true, "BR");
            respHeader = connection.getHeaderFields().toString();
            rd.close();
            
            FW.write("Reenvio da mensagem com mongoId "+whId+" recebeu como retorno o header: "+respHeader);
            FW.write("Reenvio da mensagem com mongoId "+whId+" recebeu como retorno o body: "+respBody.toString());
            
            org.w3c.dom.Document response = null;
            if(respBody.toString().contains("<?xml version=\"1.0\""))
                response = xmlToDoc(respBody.toString());
            if(response != null){
                responseId = response.getElementsByTagName("IDENTIFICATION").item(0).getTextContent();
                validResponse = validGUID(responseId);
            }
                        
            
        } catch(IOException ex){
            FW.write("Erro ao tentar realizar chamada para o endpoint "+reqEndpoint);
            FW.writeException(ex);
        } finally {
            if(connection != null)
                connection.disconnect();
        }
        
        updateWHLog(whId, tentativaAtual+1, reqDate, reqEndpoint, reqHeader, reqBody, respDate, respHeader, respBody.toString(), validResponse, responseId);
        
    }
    
    private void updateWHLog(String whId, int tentativa, String reqDate, String reqEndpoint, String reqHeader, String reqBody, String respDate, String respHeader, String respBody, boolean validResponse, String responseId){
        
        BasicDBObject query = new BasicDBObject();
        query.put("_id", new ObjectId(whId));
        
        BasicDBObject updateDocumentReq = new BasicDBObject();
        updateDocumentReq.put("data", reqDate);
        updateDocumentReq.put("head", reqHeader);
        updateDocumentReq.put("body", reqBody);
        
        BasicDBObject updateDocumentRes = new BasicDBObject();
        updateDocumentRes.put("data", respDate);
        updateDocumentRes.put("head", respHeader);
        updateDocumentRes.put("body", respBody);
        
        BasicDBObject updateDocumentHTTP = new BasicDBObject();
        updateDocumentHTTP.put("request", updateDocumentReq);
        updateDocumentHTTP.put("response", updateDocumentRes);

        BasicDBObject updateDocument = new BasicDBObject();
        if(validResponse){
            updateDocument.append("id_resposta", responseId);
        }
        updateDocument.append("endpoint", reqEndpoint);
        updateDocument.append("tentativa", tentativa);
        updateDocument.put("http", updateDocumentHTTP);
        
        BasicDBObject updateObject = new BasicDBObject();
        updateObject.put("$set", updateDocument);
        UpdateResult result = Dead_letter_timeout_SPI.COLLECTION_WH.updateOne(query, updateObject);
        if(result.getMatchedCount() > 0){
            FW.write("Webhook da mensagem com mongoId "+whId+" atualizado para: "+updateDocument);
        }
        
    }
    
    private String getCurrentDate(Boolean comHora, Boolean comDivisoes, Boolean comMilissegundos, String fuso){
        
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
    
    private org.w3c.dom.Document xmlToDoc(String xml){
        
        try{
        
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            InputSource src = new InputSource();
            src.setCharacterStream(new StringReader(xml));
            return builder.parse(src);
            
        } catch(Exception ex) {
            return null;
        }
        
    }
    
    private boolean validGUID(String guid){
        
        return guid.matches("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
        
    }

}