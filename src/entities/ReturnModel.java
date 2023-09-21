package entities;

public class ReturnModel {

    private final Boolean IS_SUCCESS;
    private final Object DATA;
    private byte[] BYTE_ARRAYS;
    private String errorMessage; // Adicione um campo para armazenar mensagens de erro.

    public ReturnModel(Boolean success, Object data) {
        this.IS_SUCCESS = success;
        this.DATA = data;
    }

    public Boolean success() {
        return this.IS_SUCCESS;
    }

    public Object getData() {
        return this.DATA;
    }

    public void setByteArrays(byte[] bytes) {
        this.BYTE_ARRAYS = bytes;
    }

    public byte[] getByteArray() {
        return this.BYTE_ARRAYS;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getErrorMessage() {
        return this.errorMessage;
    }

    @Override
    public String toString() {
        if (IS_SUCCESS) {
            return "success: true; data: " + this.DATA.toString();
        } else {
            return "success: false; error message: " + this.errorMessage;
        }
    }
}
