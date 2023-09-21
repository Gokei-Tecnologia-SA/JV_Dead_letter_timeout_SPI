package entities;

import com.dead_letter_timeout_spi.common.Globals;
import com.dead_letter_timeout_spi.common.Library;
import com.dead_letter_timeout_spi.common.Logger;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.json.JSONObject;

public class Monitor {

    private String LAST_REPORT_DATE;

    public Monitor() {
        LAST_REPORT_DATE = null;
    }

    public void reportByPhone(String phone, String reportType) {

        LAST_REPORT_DATE = Library.getDate(true, true, false, false, "BR");
        HttpURLConnection connection = null;

        try {

            URL url = new URL(Globals.getMONITOR_ENDPOINT());

            JSONObject body = new JSONObject()
                .put("numero_destino", phone)
                .put("mensagem", reportTypeToReportMessage(reportType))
                .put("resposta_usuario", false)
                .put("tipo_voz", Globals.getMONITOR_VOICE_TYPE())
                .put("bina", Globals.getMONITOR_BINA())
                .put("velocidade", Globals.getMONITOR_SPEED())
                .put("gravar_audio", false)
                .put("detecta_caixa", true)
                .put("bina_inteligente", true);

            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Access-Token", Globals.getMONITOR_TOKEN());
            connection.setUseCaches(false);
            connection.setDoOutput(true);
            connection.setConnectTimeout(Globals.getMONITOR_TIMEOUT());

            // Send request
            OutputStreamWriter wr = new OutputStreamWriter(connection.getOutputStream(), StandardCharsets.UTF_8);
            wr.write(body.toString());
            wr.close();

            // Get response
            InputStream is = connection.getInputStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(is));
            StringBuilder response = new StringBuilder();
            String line;

            while ((line = rd.readLine()) != null) {
                response.append(line);
            }

            rd.close();
            Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_INFO(), "Ligacao de alerta realizada: " + response.toString());

        } catch (Exception ex) {

            Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), "Não foi possível realizar a ligação de alerta");
            Logger.writeException(ex);

            // Registre a exceção como um erro
            Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), ex.getMessage());

        } finally {

            if (connection != null)
                connection.disconnect();

        }

    }

    public Long getSecondsSinceLastCall() {

        if (LAST_REPORT_DATE == null)
            return null;

        try {

            SimpleDateFormat sdft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date lastCallDate = sdft.parse(LAST_REPORT_DATE);
            Date currentDate = sdft.parse(Library.getDate(true, true, false, false, "BR"));

            return (currentDate.getTime() - lastCallDate.getTime()) / 1000;

        } catch (ParseException ex) {

            Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), "Não foi possível calcular os segundos desde a última ligação de alerta");
            Logger.writeException(ex);

            // Registre a exceção como um erro
            Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), ex.getMessage());

            return null;

        }

    }

    private String reportTypeToReportMessage(String reportType) {

        switch (reportType) {

            case "listener":
                return "O listener do PIX não conseguiu realizar a requisição HTTP. " + Globals.getISPB();

            case "database":
                return "O listener do PIX não conseguiu se conectar com o banco de dados. " + Globals.getISPB();

            case "report":
                return "A GOKEI não está conseguindo realizar o report das mensagens do SPI para o ISPB " + Globals.getISPB();

            default:
                return "Tipo de incidente não reportado. " + Globals.getISPB();

        }

    }
}
