package org.endeavourhealth.transform.tpp.csv.schema.clinical;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRVisit extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRVisit.class);

    public SRVisit(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT);
    }


    @Override
    protected String[] getCsvHeaders(String version) {
       if (version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK)
               || version.equals(TppCsvToFhirTransformer.VERSION_87)
               || version.equals(TppCsvToFhirTransformer.VERSION_89)) {
          return new String[]{
                  "RowIdentifier",
                  "IDOrganisationVisibleTo",
                  "DateEventRecorded",
                  "IDProfileEnteredBy",
                  "DateBooked",
                  "DateRequested",
                  "CurrentStatus",
                  "IDProfileRequested",
                  "IDProfileAssigned",
                  "FollowUpDetails",
                  "IDReferralIn",
                  "IDPatient",
                  "IDOrganisation",
                  "Duration",
                  "IDOrganisationRegisteredAt",
                  "RemovedData"
          };
       } else if (version.equals(TppCsvToFhirTransformer.VERSION_88)) {
          return new String[]{
                  "RowIdentifier",
                  "IDOrganisationVisibleTo",
                  "DateEventRecorded",
                  "IDProfileEnteredBy",
                  "DateBooked",
                  "DateRequested",
                  "CurrentStatus",
                  "IDProfileRequested",
                  "IDProfileAssigned",
                  "FollowUpDetails",
                  "IDReferralIn",
                  "IDPatient",
                  "IDOrganisation",
                  "Duration",
                  "IDOrganisationRegisteredAt"
          };
       } else {
          return new String[]{
                  "RowIdentifier",
                  "IDOrganisationVisibleTo",
                  "DateEventRecorded",
                  "IDProfileEnteredBy",
                  "DateBooked",
                  "DateRequested",
                  "CurrentStatus",
                  "IDProfileRequested",
                  "IDProfileAssigned",
                  "FollowUpDetails",
                  "IDReferralIn",
                  "IDPatient",
                  "IDOrganisation",
                  "Duration",
                  "IDOrganisationRegisteredAt"
          };
       }
    }

    public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
    public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
    public CsvCell getDateEventRecorded() { return super.getCell("DateEventRecorded");}
    public CsvCell getIDProfileEnteredBy() { return super.getCell("IDProfileEnteredBy");}
    public CsvCell getDateBooked() { return super.getCell("DateBooked");}
    public CsvCell getDateRequested() { return super.getCell("DateRequested");}
    public CsvCell getCurrentStatus() { return super.getCell("CurrentStatus");}
    public CsvCell getIDProfileRequested() { return super.getCell("IDProfileRequested");}
    public CsvCell getIDProfileAssigned() { return super.getCell("IDProfileAssigned");}
    public CsvCell getFollowUpDetails() { return super.getCell("FollowUpDetails");}
    public CsvCell getIDReferralIn() { return super.getCell("IDReferralIn");}
    public CsvCell getIDPatient() { return super.getCell("IDPatient");}
    public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");}
    public CsvCell getDuration() { return super.getCell("Duration");}
    public CsvCell getIDOrganisationRegisteredAt() { return super.getCell("IDOrganisationRegisteredAt");}
    public CsvCell getRemovedData() { return super.getCell("RemovedData");}


    //TODO fix the string below to make it meaningful
    @Override
    protected String getFileTypeDescription() {return "TPP Visit Entry file ";}

    @Override
    protected boolean isFileAudited() {return true;}
}
