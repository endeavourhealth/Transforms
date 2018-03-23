package org.endeavourhealth.transform.tpp;

import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.transform.common.HasServiceSystemAndExchangeIdI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class TppCsvHelper implements HasServiceSystemAndExchangeIdI {
    private static final Logger LOG = LoggerFactory.getLogger(TppCsvHelper.class);

    //private static final String CODEABLE_CONCEPT = "CodeableConcept";
    private static final String ID_DELIMITER = ":";

    private static final ParserPool PARSER_POOL = new ParserPool();

    private final UUID serviceId;
    private final UUID systemId;
    private final UUID exchangeId;
    private final String dataSharingAgreementGuid;
    private final boolean processPatientData;


    public TppCsvHelper(UUID serviceId, UUID systemId, UUID exchangeId, String dataSharingAgreementGuid, boolean processPatientData) {
        this.serviceId = serviceId;
        this.systemId = systemId;
        this.exchangeId = exchangeId;
        this.dataSharingAgreementGuid = dataSharingAgreementGuid;
        this.processPatientData = processPatientData;
    }


    @Override
    public UUID getServiceId() {
        return serviceId;
    }

    @Override
    public UUID getSystemId() {
        return systemId;
    }

    @Override
    public UUID getExchangeId() {
        return exchangeId;
    }

    public String getDataSharingAgreementGuid() {
        return dataSharingAgreementGuid;
    }

    public boolean isProcessPatientData() {
        return processPatientData;
    }

}