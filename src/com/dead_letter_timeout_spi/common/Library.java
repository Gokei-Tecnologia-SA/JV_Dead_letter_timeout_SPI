package com.dead_letter_timeout_spi.common;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;
import java.util.regex.Pattern;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.NoSuchPaddingException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.json.JSONArray;
import org.json.JSONObject;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class Library {
    
    public static String getDate(Boolean withTime, Boolean withDivisions, Boolean withMilli, Boolean useSpecialChars, String timezone) {

        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        OffsetDateTime nowTruncated = now.truncatedTo(ChronoUnit.MILLIS);
        
        if(!timezone.equals("UTC"))
            nowTruncated = nowTruncated.minusHours(3);
                
        String format = createDateFormat(withTime, withDivisions, withMilli, useSpecialChars);
        nowTruncated.format(DateTimeFormatter.ofPattern(format));
        
        String result = nowTruncated.toString();
        if(withTime && withDivisions && withMilli && useSpecialChars && result.length() == 20){
            result = result.substring(0, 19)+".000Z";
        }
        
        return result;

    }
    
    public static int getDay() {
        
        ZonedDateTime currentSystemTime = ZonedDateTime.now();
        ZonedDateTime currentUTCTime = currentSystemTime.withZoneSameInstant(ZoneOffset.UTC);
        return currentUTCTime.getDayOfMonth();
        
    }
    
    public static int getMonth() {
        
        ZonedDateTime currentSystemTime = ZonedDateTime.now();
        ZonedDateTime currentUTCTime = currentSystemTime.withZoneSameInstant(ZoneOffset.UTC);
        return currentUTCTime.getMonthValue();
        
    }
    
    public static int getYear() {
        
        ZonedDateTime currentSystemTime = ZonedDateTime.now();
        ZonedDateTime currentUTCTime = currentSystemTime.withZoneSameInstant(ZoneOffset.UTC);
        return currentUTCTime.getYear();
        
    }
    
    private static String createDateFormat(Boolean withTime, Boolean withDivisions, Boolean withMilliseconds, Boolean useSpecialChars) {
        
        String format;
        if (withTime && withDivisions && withMilliseconds && useSpecialChars) {
            format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        } else if(withTime && withDivisions && withMilliseconds && !useSpecialChars){
            format = "yyyy-MM-dd HH:mm:ss.SSS";
        } else if (withTime && withDivisions && !withMilliseconds) {
            format = "yyyy-MM-dd HH:mm:ss";
        } else if (withTime && !withDivisions && withMilliseconds) {
            format = "yyyyMMddHHmmssSSS";
        } else if (withTime && !withDivisions && !withMilliseconds) {
            format = "yyyyMMddHHmmss";
        } else if (!withTime && withDivisions) {
            format = "yyyy-MM-dd";
        } else if (!withTime && !withDivisions) {
            format = "yyyyMMdd";
        } else {
            format = "yyyy-MM-dd HH:mm:ss.SSS";
        }
        
        return format;
        
    }
    
    private static TimeZone convertTimezone(String timezone) {
        
        switch (timezone) {
            case "UTC":
                return TimeZone.getTimeZone("UTC");
            case "BR":
                return TimeZone.getTimeZone("America/Sao_Paulo");
            default:
                return TimeZone.getTimeZone("America/Sao_Paulo");
        }
        
    }

    private static Key getPropertiesKey() {
        
        try {
            
            FileInputStream keyFileInputStream = new FileInputStream(Globals.getPropertiesKeyName());
            ObjectInputStream objectInputStream = new ObjectInputStream(keyFileInputStream);
            Key aesKey = (Key) objectInputStream.readObject();
            objectInputStream.close();
            keyFileInputStream.close();
            return aesKey;
            
        } catch (IOException | ClassNotFoundException ex) {
            Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), "Não foi possível recuperar a chave das propriedades");
            Logger.writeException(ex);
            System.exit(1);
        }
        
        return null;
        
    } 
    
    public static Properties getProperties(String path) {
        
        try {
            
            // Get AES Key
            Key secretKey = getPropertiesKey();
            // Create Cipher
            Cipher aesCipher = Cipher.getInstance("AES");
            aesCipher.init(Cipher.DECRYPT_MODE, secretKey);
            // Open encrypted file
            FileInputStream fis = new FileInputStream(path);
            // Decode file
            CipherInputStream cis = new CipherInputStream(fis, aesCipher);
            // Create Properties
            Properties properties = new Properties();
            properties.load(cis);
            
            cis.close();
            fis.close();
            
            return properties;
            
        } catch (IOException | InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException ex) {
            
            Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), "Não foi possível decodificar o arquivo de propriedades");
            Logger.writeException(ex);
            System.exit(1);
            
        }
        
        return null;
        
    }
 
    public static String handleErrorText(String text) {
        
        return text.toUpperCase();

    }
    
    public static void sleep(Integer milliseconds) {
        
        try {
            
            Thread.sleep(milliseconds);
            
        } catch (InterruptedException ex) {
            
            Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), "Não foi possível realizar o sleep na thread");
            Logger.writeException(ex);
            
        }
        
    }
    
    public static String generateHttpReturnMessage(Long time, Long code, String head, String body) {
        
        try{
            
            JSONArray bodyMessages = null;
            String contentType = getDataFromHTTPHeader(head, "Content-Type");
            if(contentType != null){
                String boundary = contentType.split("boundary=")[1];
                bodyMessages = splitHttpMessages(boundary, body);
            }

            JSONObject returnMessage = new JSONObject()
                .put("code", code)
                .put("time", time)
                .put("head", head)
                .put("body", bodyMessages);

            return returnMessage.toString();
            
        } catch (Exception ex){
            
            Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), "Não foi possível tratar o retorno http");
            Logger.writeException(ex);
            return new JSONObject().toString();
                    
        }
        
    }
    
    public static String getDataFromHTTPHeader(String header, String target) {
        
        String[] headerData = header.split("\r\n");
        Integer size = headerData.length;
        
        String data = null;
        for(int i = 0; i < size; i++){
            if(headerData[i].contains(target))
                data = headerData[i].trim().split(":")[1].trim();
        }
 
        return data; 
        
    }
    
    public static JSONArray splitHttpMessages(String boundary, String body){
        
        JSONArray returnMessages = new JSONArray();
        
        // Split messages by boundary code
        String[] messages = body.split("--"+boundary);
        int size = messages.length;
        
        // For each message of the response
        for(int i = 0; i < size; i++){
            
            if(messages[i].equals("--"))
                continue;
            
            String[] messagesInfo = messages[i].split("Content-Type: application/xml;charset=utf-8");
            int subSize = messagesInfo.length;
            for(int j = 0; j < subSize; j++){
                messagesInfo[j] = messagesInfo[j].trim();
            }

            if(messagesInfo.length == 2){
                returnMessages.put(
                    new JSONObject()
                        .put("PI-ResourceId", messagesInfo[0].split(": ")[1])
                        .put("xml", messagesInfo[1])
                );
            }
            
        }
        
        return returnMessages;
        
    }
    
    public static Document xmlStringToDocument(String xml) {
        
        try {

            DocumentBuilder builder = DocumentBuilderFactory
                .newInstance()
                .newDocumentBuilder();

            InputSource src = new InputSource();
            src.setCharacterStream(new StringReader(xml));
            Document doc = builder.parse(src);
            return doc;

        } catch (IOException | ParserConfigurationException | SAXException ex) {
            
            Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), "Não foi possível converter o XML para Document: "+xml);
            Logger.writeException(ex);
            return null;
        
        }

    }
    
    public static Document nodeToDocument(Node node) {
        
        try {
            
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document newDocument = builder.newDocument();
            Node importedNode = newDocument.importNode(node, true);
            newDocument.appendChild(importedNode);
            return newDocument;
            
        } catch(ParserConfigurationException | DOMException ex) {
            
            Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), "Não foi possível converter o Node para Document: "+node.toString());
            Logger.writeException(ex);
            return null;
            
        }
        
    }
    
    public static String generateRandomAlphanumericString(int size) {
 
        int leftLimit = 48;
        int rightLimit = 122;
        Random random = new Random();

        return random.ints(leftLimit, rightLimit + 1)
            .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
            .limit(size)
            .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
            .toString();
        
    }
    
    public static String convertStatus(String originalStatus) {
        
        switch(originalStatus){
            case "REJT":
                return "SOLICITACAO_REJEITADA";
            case "COMP":
                return "SOLICITACAO_COMPLETADA";
            case "QUED":
                return "SOLICITACAO_SUSPENSA";
            case "ACCC":
                 return "TRANSACAO_RECEBIDA";
            case "ACSC":
                return "TRANSACAO_CONCLUIDA";
            case "ACSP":
                return "TRANSACAO_VALIDADA_PELO_RECEBEDOR";
            case "RJCT":
                return "TRANSACAO_REJEITADA";
            default:
                return "DESCONHECIDO";
        }
        
    }
    
    public static String convertStatusIniciacao(String originalStatus) {
        
        switch(originalStatus){
            case "ACSP":
                return "INICIACAO_ACEITA";
            case "RJCT":
                return "INICIACAO_REJEITADA";
            default:
                return "DESCONHECIDO";
        }
        
    }
    
    public static String extractISPBFromEndToEnd(String endToEndId) {
        
        if(endToEndId.length() == 32){
            return endToEndId.substring(1, 9);
        }
        
        return null;
        
    }
    
    public static String getDefaultDescriptionForErrorCode(String type, String errorCode) {
        
        switch (type) {
            case "transaction":
                switch(errorCode){
                    case "AB09":
                        return "Transação interrompida devido a erro no participante do usuário recebedor.";
                    case "AC03":
                        return "Número da agência e/ou conta transacional do usuário recebedor inexistente ou inválido.";
                    case "AC06":
                        return "Conta transacional do usuário recebedor encontra-se bloqueada.";
                    case "AC07":
                        return "Número da conta transacional do usuário recebedor encerrada.";
                    case "AC14":
                        return "Tipo incorreto para a conta transacional do usuário recebedor.";
                    case "AG03":
                        return "Tipo de transação não é suportado/autorizado na conta transacional do usuário recebedor.";
                    case "AM02":
                        return "Ordem em valor que faz superar o limite permitido para o tipo de conta transacional creditada.";
                    case "AM09":
                        return "Devolução de pagamento em valor que faz superar o valor da ordem de pagamento instantâneo correspondente.";
                    case "BE01":
                        return "CPF/CNPJ do usuário recebedor não é consistente com o titular da conta transacional especificada.";
                    case "BE17":
                        return "QR Code rejeitado pelo participante do usuário recebedor.";
                    case "CH11":
                        return "CPF/CNPJ do usuário recebedor incorreto.";
                    case "DS04":
                        return "Ordem rejeitada pelo participante do usuário recebedor.";
                    case "DS24":
                        return "Ordem rejeitada por extrapolação do tempo decorrido entre o envio da pain.013 e recebimento da pacs.008.";
                    case "DT05":
                        return "Transação extrapola o prazo máximo para devolução de pagamento instantâneo regulamentado pelo Pix.";
                    case "ED05":
                        return "Erro no processamento do pagamento instantâneo  (erro genérico).";
                    case "RR04":
                        return "Ordem de pagamento com usuário pagador sancionado pelo Conselho de Segurança das Nações Unidas (CSNU).";
                    case "SL02":
                        return "A transação original não está relacionada aos serviços de Pix Saque ou Pix Troco.";
                    default:
                        return "Erro no processamento do pagamento instantâneo  (erro genérico).";
                }
            case "initiation":
                switch(errorCode){
                    case "AB10":
                        return "Transação interrompida devido a erro no participante do usuário pagador.";
                    case "AC02":
                        return "Número da conta transacional do usuário pagador inexistente ou inválido.";
                    case "AC06":
                        return "Conta transacional do usuário pagador encontra-se bloqueada.";
                    case "AC05":
                        return "Número da conta transacional do usuário pagador encerrada.";
                    case "AC13":
                        return "Tipo incorreto para a conta transacional do usuário pagador.";
                    case "AM02":
                        return "Iniciação de pagamento em valor que faz superar o limite permitido para o tipo de conta transacional debitada.";
                    case "BE16":
                        return "QR Code rejeitado pelo participante do usuário pagador.";
                    case "DS04":
                        return "Iniciação de pagamento rejeitada pelo participante do usuário pagador.";
                    case "RR10":
                        return "Incompatibilidade entre as informações contidas na mensagem de iniciação de pagamento e os parâmetros de pagamento aplicáveis ao QR Code.";
                    default:
                        return "Iniciação de pagamento rejeitada pelo participante do usuário pagador.";
                }
            default:
                return "UNDEFINED";
        }
        
    }
 
    public static Document generateDocument() {
        
        try {
            
            DocumentBuilderFactory documentFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentFactory.newDocumentBuilder();
            return documentBuilder.newDocument();
            
        } catch (ParserConfigurationException ex) {
        
            Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), "Não foi possível gerar um novo Document");
            Logger.writeException(ex);
            return null;
        
        }
        
    }
    
    public static String convertDocumentIntoXML(Document doc) {
        
        try {
            
            StringWriter sw = new StringWriter();
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
            transformer.setOutputProperty(OutputKeys.METHOD, "XML");
            transformer.setOutputProperty(OutputKeys.INDENT, "no");
            transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            transformer.transform(new DOMSource(doc), new StreamResult(sw));
            return sw.toString();
            
        } catch(IllegalArgumentException | TransformerException ex) {
            
            Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), "Não foi possível converter o Document para XML: "+doc);
            Logger.writeException(ex);
            return null;
            
        }
        
    }
    
    public static String generateMessageId(String ispb) {
        
        return "M"+ispb+getAlphaNumericString(23);
        
    }
    
    public static String getAlphaNumericString(int n) {
  
        String AlphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvxyz";
        StringBuilder sb = new StringBuilder(n);
  
        for (int i = 0; i < n; i++) {
            int index = (int)(AlphaNumericString.length() * Math.random());
            sb.append(AlphaNumericString.charAt(index));
        }
  
        return sb.toString();
        
    }
    
    public static String getMessageTypeLabel(String code) {
        
        switch(code){
            
            case "admi002":
                return "Controle";
                
            case "pacs008":
            case "pacs004":
            case "pacs002":
                return "Transferência de recursos";
                
            case "pain013":
            case "pain014":
                return "Iniciação de pagamento";
                
            case "camt052":
            case "camt053":
            case "camt054":
            case "camt060":
                return "Gestão PI";
                
            case "reda014":
            case "reda016":
            case "reda017":
            case "reda031":
                return "Gestão indireto";
                
            case "reda022":
                return "Atualização de responsáveis";
                
            case "reda041":
            case "camt014":
            case "camt004":
                return "Avisos";
                
            case "pibr001":
            case "pibr002":
                return "Teste de conectividade";
                
            default:
                return "Desconhecido";
            
        }
        
    }
    
    public static String getStringFromJson(JSONObject json, String target) {
        return json.has(target) ? (String) json.get(target) : null;
    }
    
    public static int getIntFromJson(JSONObject json, String target) {
        return json.has(target) ? (int) json.get(target) : null;
    }

    public static JSONObject getJSONObjectFromJson(JSONObject json, String target) {
        return json.has(target) ? (JSONObject) json.get(target) : null;
    }
    
    public static JSONObject generateDefaultErrosResponsePacs008() {
        
        return new JSONObject()
            .put("status", "RJCT")
            .put("error", new JSONObject()
                .put("code", "ED05")
                .put("description", "Erro no processamento do pagamento instantâneo  (erro genérico)."));
                
    }
    
    public static String removeQuebrasLinha(String original) {
        return original.replaceAll("[\r\n]+", "");
    }
    
    public static void makeEmergencyCall(String reportType){
        
        Long sdsSinceLastCall = Globals.getMONITOR().getSecondsSinceLastCall();
        if(sdsSinceLastCall == null || sdsSinceLastCall >= Globals.getEMERGENCY_INTERVAL()){
            int size = Globals.getEMERGENCY_NUMBERS().length;
            for(int i = 0; i < size; i++){
                Globals.getMONITOR().reportByPhone(
                    Globals.getEMERGENCY_NUMBERS()[i], 
                    reportType);
            }
        }
        
    }
    
    public static boolean isGUIDValid(String input) {
        
        String guidPattern = "^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$";
        return Pattern.matches(guidPattern, input);
        
    }
    
    public static HashSet<String> generateReportablesMessagesHashSet(String[] messages){

        HashSet<String> reportableSet = new HashSet<>();
        reportableSet.addAll(Arrays.asList(messages));
        return reportableSet;
        
    }
    
    public static boolean reportableMessage(String messageCode){

        if(messageCode == null)
            return false;
        
        return Globals.getREPORTABLE_MESSSAGES().contains(messageCode);
        
    }
    
    public static boolean isResponseMessage(String messageCode){
        
        switch (messageCode) {
            case "admi002":
            case "camt052":
            case "camt053":
            case "camt054":
            case "pacs002":
            case "pain014":
            case "reda016":
                return true;
            default:
                return false;
        }
        
    }
    
    public static org.bson.Document getIndex(int order){
        
        org.bson.Document index = new org.bson.Document("_id", order);
        return index;
        
    }
    
    public static org.bson.Document getDefaultSort(int order){
        
        org.bson.Document sort = new org.bson.Document("_id", order);
        return sort;
        
    }
    
    public static String getCustomWebhookForMessage(String messageCode){
        
        Properties props = Library.getProperties(Globals.getPROPERTIES_FILE());
        if(props == null)
            return null;
        
        String endpoint = props.getProperty("report_custom_endpoint_"+messageCode+"_"+Globals.getENVIRONMENT());
        return endpoint;
        
    }
    
}