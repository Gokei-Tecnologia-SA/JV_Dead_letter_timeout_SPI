package entities;

import com.dead_letter_timeout_spi.common.Library;
import dead_letter_timeout_spi.Dead_letter_timeout_SPI;
import static dead_letter_timeout_spi.Dead_letter_timeout_SPI.FW;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import org.bson.BsonValue;

public class WebhookManager {

    private BsonValue ID_WH_LOG;
    private String INSERTED_MESSAGE_ID;
    private String REQUEST_ID;
    public String WEBHOOKURL = Dead_letter_timeout_SPI.WEBHOOKURL;
    public static String lastHttpResponse;
    public String getLastHttpResponse() {
    return lastHttpResponse;
    }

    public String sendReportRequest(String messageCode, String messageId, String messageVersion, String xmlModificado, int currentAttempt, String idempotencyKey) {

        HttpURLConnection connection = null;
        String requestDate = Library.getDate(true, true, true, false, "BR");
        String responseDate = "";
        String responseHeader = "";

        try {

            URL url = new URL(WEBHOOKURL);

            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");

            connection.setRequestProperty("content-type", "application/xml");
            connection.addRequestProperty("idempotencykey", idempotencyKey);
            connection.addRequestProperty("spi-message-id", "M0000000000000000000000000000000");
            connection.addRequestProperty("spi-message-code", "admi.002");
            connection.addRequestProperty("spi-message-version", "admi.002.spi.1.3");
            connection.addRequestProperty("current-attempt", String.valueOf(currentAttempt));

            connection.setUseCaches(false);
            connection.setDoOutput(true);
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            String requestHeader = connection.getRequestProperties().toString();

            OutputStreamWriter wr = new OutputStreamWriter(connection.getOutputStream(), StandardCharsets.UTF_8);
            wr.write(xmlModificado);
            wr.close();

            InputStream is = connection.getInputStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(is));
            StringBuilder httpResponse = new StringBuilder();
            String line;

            while ((line = rd.readLine()) != null) {
                httpResponse.append(line);
            }
            
            System.out.println("Resposta do Webhook: " + httpResponse.toString());


            responseHeader = connection.getHeaderFields().toString();
            rd.close();

            responseDate = Library.getDate(true, true, true, false, "BR");

            ID_WH_LOG = Dead_letter_timeout_SPI.MDB.saveWHLog(
                    WEBHOOKURL,
                    INSERTED_MESSAGE_ID != null ? INSERTED_MESSAGE_ID : "NULL",
                    xmlModificado,
                    httpResponse.toString(),
                    REQUEST_ID,
                    requestDate,
                    requestHeader,
                    responseDate,
                    responseHeader,
                    currentAttempt,
                    "REPORT"
            );
            lastHttpResponse = httpResponse.toString();
            return httpResponse.toString();

        } catch (IOException ex) {
            // Trate exceções de IO especificamente, como SocketTimeoutException, IOException, etc.
            FW.writeException(ex);
        } catch (Exception ex) {
            // Trate outras exceções aqui
            FW.writeException(ex);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }

        return null;

    }

    public BsonValue getID_WH_LOG() {
        return ID_WH_LOG;
    }

    public String getINSERTED_MESSAGE_ID() {
        return INSERTED_MESSAGE_ID;
    }

    public void setINSERTED_MESSAGE_ID(String INSERTED_MESSAGE_ID) {
        this.INSERTED_MESSAGE_ID = INSERTED_MESSAGE_ID;
    }

    public String getREQUEST_ID() {
        return REQUEST_ID;
    }

    public void setREQUEST_ID(String REQUEST_ID) {
        this.REQUEST_ID = REQUEST_ID;
    }

    String WebHookUrl() {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

}
