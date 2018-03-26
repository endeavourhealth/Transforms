package org.endeavourhealth.transform.tpp.csv.schema.unused.mentalhealth;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRECTTreatment extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRECTTreatment.class); 

  public SRECTTreatment(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "TreatmentDate",
                      "RecoveryPerson",
                      "OdpPerson",
                      "Anaesthetist",
                      "TreatingDoctor",
                      "DateCreation",
                      "IDProfileCreatedBy",
                      "IDECTCourse",
                      "IDEvent",
                      "IDPatient",
                      "IDOrganisation"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getTreatmentDate() { return super.getCell("TreatmentDate");};
 public CsvCell getRecoveryPerson() { return super.getCell("RecoveryPerson");};
 public CsvCell getOdpPerson() { return super.getCell("OdpPerson");};
 public CsvCell getAnaesthetist() { return super.getCell("Anaesthetist");};
 public CsvCell getTreatingDoctor() { return super.getCell("TreatingDoctor");};
 public CsvCell getDateCreation() { return super.getCell("DateCreation");};
 public CsvCell getIDProfileCreatedBy() { return super.getCell("IDProfileCreatedBy");};
 public CsvCell getIDECTCourse() { return super.getCell("IDECTCourse");};
 public CsvCell getIDEvent() { return super.getCell("IDEvent");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRECTTreatment Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
