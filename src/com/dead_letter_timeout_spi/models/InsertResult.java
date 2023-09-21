package com.dead_letter_timeout_spi.models;

import org.bson.BsonValue;

public class InsertResult {

    private final String messageIdentificator;
    private final BsonValue insertId;
    private final String originalResourceId;
    private final String originalMessageId;
    private final String originalInstructionId;
    private final String originallEndToEndId;

    public InsertResult(String messageIdentificator, BsonValue insertId, String originalResourceId, String originalMessageId, String originalInstructionId, String originallEndToEndId) {
        this.messageIdentificator = messageIdentificator;
        this.insertId = insertId;
        this.originalResourceId = originalResourceId;
        this.originalMessageId = originalMessageId;
        this.originalInstructionId = originalInstructionId;
        this.originallEndToEndId = originallEndToEndId;
    }

    public String getMessageIdentificator() {
        return messageIdentificator;
    }

    public BsonValue getInsertId() {
        return insertId;
    }

    public String getOriginalResourceId() {
        return originalResourceId;
    }

    public String getOriginalMessageId() {
        return originalMessageId;
    }

    public String getOriginalInstructionId() {
        return originalInstructionId;
    }

    public String getOriginallEndToEndId() {
        return originallEndToEndId;
    }

}