package entities;

import br.com.trueaccess.TacException;
import br.com.trueaccess.TacNDJavaLib;
import com.dinamonetworks.Dinamo;
import com.dinamonetworks.PIXHTTPReqDetails;
import com.dinamonetworks.PIXResponse;
import com.dead_letter_timeout_spi.common.Globals;
import com.dead_letter_timeout_spi.common.Library;
import com.dead_letter_timeout_spi.common.Logger;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

public class HSM {
    
    private final Dinamo DINAMO_API;
    private final String BASE_URL;
    private final String BASE_HOST;
    
    public HSM(){
        
        DINAMO_API = new Dinamo();
        BASE_URL = Globals.getENVIRONMENT().equals("prod") ? "https://arq.pi.rsfn.net.br:1130/" : "https://arq-h.pi.rsfn.net.br:1130/";
        BASE_HOST = Globals.getENVIRONMENT().equals("prod") ? "arq.pi.rsfn.net.br" : "arq-h.pi.rsfn.net.br";
    
    }
    
    private ReturnModel openSession() {
        
        try {
            
            DINAMO_API.openSession(
                "",
                Globals.getHSM_USER(),
                decodePassword(Globals.getHSM_PASSWORD())
            );
            
            return new ReturnModel(true, DINAMO_API.getCID());
            
        } catch (TacException ex) {
            
            Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), "Não foi possível abrir uma sessão no HSM");
            Logger.writeException(ex);
            return new ReturnModel(false, ex.getMessage());
            
        }
        
    }
    
    private ReturnModel closeSession(){
        
        try {
            
            DINAMO_API.closeSession();
            return new ReturnModel(true, "Sessao finalizada");
            
        } catch (TacException ex) {
            
            Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), "Não foi possível fechar uma sessão no HSM");
            Logger.writeException(ex);     
            return new ReturnModel(false, ex.getMessage());
        
        }
        
    }
    
    private String decodePassword(String encodedPassword){
        
        byte[] decodedByte = Base64.getDecoder().decode(encodedPassword);
        String decodedPassword = new String(decodedByte);
        return new StringBuilder(decodedPassword).reverse().toString();

    }

    public ReturnModel sendHttp(String url, String method){
        
        ReturnModel session = openSession();
        if(session.success()){
            
            PIXResponse response;
            String header[] = {
                "Host: "+Globals.getPIX_HOST(),
                "Transfer-Encoding: chunked",
                "Content-Encoding: gzip",
                "Content-Type: ",
                "Accept-Encoding: gzip",
                "Connection: keep-alive",
                "User-Agent: GOKEI TECNOLOGIA v"+Globals.getVERSION()+" - "+Globals.getISPB(),
                "Accept: multipart/mixed"
            };

            Logger.printHTTPRequestInfo(url, method, Arrays.toString(header));
            try{

                switch (method) {

                    case "GET":
                        response = DINAMO_API.getPIX(
                            Globals.getHSM_PIC_KEY(),
                            Globals.getHSM_PIC_CER(),
                            Globals.getHSM_CHAIN_PIC(),
                            url,
                            header,
                            Globals.getTIMEOUT_PIX(),
                            TacNDJavaLib.PIX_GZIP);
                        break;

                    case "DELETE":
                        response = DINAMO_API.deletePIX(
                            Globals.getHSM_PIC_KEY(),
                            Globals.getHSM_PIC_CER(),
                            Globals.getHSM_CHAIN_PIC(),
                            url,
                            header,
                            Globals.getTIMEOUT_PIX(),
                            TacNDJavaLib.PIX_GZIP);
                        break;

                    default:
                        closeSession();
                        String msg = "Metodo "+method+" indisponivel para requisição HTTP PIX";
                        Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_WARNING(), msg);
                        return new ReturnModel(false, msg);

                }

                PIXHTTPReqDetails details = DINAMO_API.getPIXHTTPReqDetails();
                Long time = details.getTotalTime();
                Long httpCode = details.getHttpResponseCode();
                String headResponse = new String(response.getHead());
                String bodyResponse = new String(response.getBody());

                if(httpCode == 200 && method.equals("GET")){
                    Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_INFO(), "(Requisição HTTP PIX) Header: "+Arrays.toString(header)+", Method: "+method+", URL: "+url);
                    Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_INFO(), "(Resposta requisição HTTP PIX) Origin: "+url+", Header: "+headResponse+", Body: "+bodyResponse+", URL: "+url);
                } else {
                    if(httpCode != 204){
                        Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_WARNING(), "(Requisição HTTP PIX com código diferente de 200) Header: "+Arrays.toString(header)+", Method: "+method+", URL: "+url);
                        Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_INFO(), "(Resposta requisição HTTP PIX com código diferente de 200) Origin: "+url+", Header: "+headResponse+", Body: "+bodyResponse+", URL: "+url);
                    }
                }
                
                String returnMessage = Library.generateHttpReturnMessage(
                    time,
                    httpCode,
                    headResponse,
                    bodyResponse
                );
                
                closeSession();
                return new ReturnModel(true, returnMessage);

             } catch(TacException ex){

                closeSession();
                Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), "Não foi possível realizar uma requisição HTTP no HSM");
                Logger.writeException(ex);
                return new ReturnModel(false, ex.getMessage());

            }
            
        }
        
        Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_WARNING(), (String) session.getData());
        return new ReturnModel(false, (String) session.getData());
                
    }
    
    public ReturnModel verifyPixSignature(byte[] message){
        
        ReturnModel session = openSession();
        if(session.success()){
            
            try{
                
                Boolean valid = this.DINAMO_API.verifyPIX(
                    Globals.getHSM_CHAIN_PIA(),
                    null,
                    message);
                
                closeSession();
                return new ReturnModel(true, valid);
                
            } catch(Exception ex){
                
                closeSession();
                Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), "Não foi possível verificar a assinatura PIX da mensagem no HSM");
                Logger.writeException(ex);
                return new ReturnModel(false, ex.getMessage());
                
            }
  
        }
        
        Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_WARNING(), (String) session.getData());
        return new ReturnModel(false, (String) session.getData());
        
    }
    
    public ReturnModel signXMLPIX(String xml){
        
        ReturnModel session = openSession();
        if(session.success()){
        
            try {
                        
                byte[] bytes_xml = xml.getBytes(StandardCharsets.UTF_8);
                byte[] xml_assinado = this.DINAMO_API.signPIX(
                    Globals.getHSM_PIA_KEY(),
                    Globals.getHSM_PIA_CER(),
                    bytes_xml
                );

                ReturnModel exec_return = new ReturnModel(true, "Signed XML");
                exec_return.setByteArrays(xml_assinado);
                return exec_return;

            } catch (TacException ex) {
                
                closeSession();
                Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), "Não foi possível realizar a assinatura da mensagem PIX no HSM");
                Logger.writeException(ex);
                return new ReturnModel(false, ex.getMessage());
            
            }
            
        }
        
        Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_WARNING(), (String) session.getData());
        return new ReturnModel(false, (String) session.getData());
        
    }
    
    public String downloadFile(String url){
        
        ReturnModel session = openSession();
        if(session.success()){
            
            String strURL = BASE_URL+"api/v1/download/"+Globals.getISPB()+"/extrato/"+url+".zip";
            String[] header = {"Host: "+this.BASE_HOST, "Accept: ", "Expect: ", "User-Agent: "};

            try{

                PIXResponse response = DINAMO_API.getPIX(Globals.getHSM_PIC_KEY(),
                    Globals.getHSM_PIC_CER(),
                    Globals.getHSM_CHAIN_PIC(),
                    strURL,
                    header,
                    10000,
                    0);

                Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_INFO(), "Head da requisição de download do arquivo "+url+": "+new String(response.getHead()));
                Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_INFO(), "Body da requisição de download do arquivo "+url+": "+new String(response.getBody()));

                PIXHTTPReqDetails details = DINAMO_API.getPIXHTTPReqDetails();
                long responseCode = details.getHttpResponseCode();

                if(responseCode == 200){

                    File outputFile = new File(url+".zip");
                    FileOutputStream outputStream = new FileOutputStream(outputFile);
                    outputStream.write(response.getBody());
                    closeSession();
                    return url+".zip";

                } else {

                    File outputFile = new File(url);
                    FileOutputStream outputStream = new FileOutputStream(outputFile);
                    outputStream.write(response.getBody());
                    closeSession();
                    return url;

                }


            } catch (Exception ex) {

                Logger.writeError(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_ERROR(), "Não foi possível realizar a requisição HTTP para baixar o arquivo");
                Logger.writeException(ex);

            }

            closeSession();
            return null;

        } else {
            
            Logger.write(Thread.currentThread().getStackTrace(), Globals.getLOG_LABEL_WARNING(), (String) session.getData());
            return null;
        
        }
        
    }
    
    @Override
    public String toString(){
     
        return "["+DINAMO_API+"]";
    
    }
    
}
