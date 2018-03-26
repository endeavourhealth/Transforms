package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRService extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRService.class); 

  public SRService(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
            super(serviceId, systemId, exchangeId, version, filePath,
                    TppCsvToFhirTransformer.CSV_FORMAT,
                    TppCsvToFhirTransformer.DATE_FORMAT,
                    TppCsvToFhirTransformer.TIME_FORMAT);
        }


        @Override
        protected String[] getCsvHeaders(String version) {
            return new String[]{
                      "RowIdentifier",
                      "IDOrganisationVisibleTo",
                      "DateEventRecorded",
                      "DateEvent",
                      "IDProfileEnteredBy",
                      "IDDoneBy",
                      "TextualEventDoneBy",
                      "IDOrganisationDoneAt",
                      "ActionName",
                      "ServiceName",
                      "ID",
                      "ServiceType",
                      "ServiceCategory",
                      "IDPatientRelationshipCarer",
                      "IDAssessment",
                      "IndicativeServiceCost",
                      "TrustFunded",
                      "DirectPayment",
                      "DateApproved",
                      "IDProfileApproved",
                      "EndReason",
                      "IDOrganisation",
                      "IDEvent",
                      "IDPatient"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getDateEventRecorded() { return super.getCell("DateEventRecorded");};
 public CsvCell getDateEvent() { return super.getCell("DateEvent");};
 public CsvCell getIDProfileEnteredBy() { return super.getCell("IDProfileEnteredBy");};
 public CsvCell getIDDoneBy() { return super.getCell("IDDoneBy");};
 public CsvCell getTextualEventDoneBy() { return super.getCell("TextualEventDoneBy");};
 public CsvCell getIDOrganisationDoneAt() { return super.getCell("IDOrganisationDoneAt");};
 public CsvCell getActionName() { return super.getCell("ActionName");};
 public CsvCell getServiceName() { return super.getCell("ServiceName");};
 public CsvCell getID() { return super.getCell("ID");};
 public CsvCell getServiceType() { return super.getCell("ServiceType");};
 public CsvCell getServiceCategory() { return super.getCell("ServiceCategory");};
 public CsvCell getIDPatientRelationshipCarer() { return super.getCell("IDPatientRelationshipCarer");};
 public CsvCell getIDAssessment() { return super.getCell("IDAssessment");};
 public CsvCell getIndicativeServiceCost() { return super.getCell("IndicativeServiceCost");};
 public CsvCell getTrustFunded() { return super.getCell("TrustFunded");};
 public CsvCell getDirectPayment() { return super.getCell("DirectPayment");};
 public CsvCell getDateApproved() { return super.getCell("DateApproved");};
 public CsvCell getIDProfileApproved() { return super.getCell("IDProfileApproved");};
 public CsvCell getEndReason() { return super.getCell("EndReason");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};
 public CsvCell getIDEvent() { return super.getCell("IDEvent");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRService Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
