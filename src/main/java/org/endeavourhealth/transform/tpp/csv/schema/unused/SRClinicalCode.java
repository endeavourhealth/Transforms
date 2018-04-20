package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRClinicalCode extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRClinicalCode.class); 

  public SRClinicalCode(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "DateCreated",
                      "IdProfileCreatedBy",
                      "TypeOfEpisode",
                      "IDProfileConsultant",
                      "DateEpisodeStart",
                      "DateEpisodeEnd",
                      "DateAuthorised",
                      "IDAppointment",
                      "IDAttendance",
                      "IDConsultantEvent",
                      "IDPatientRegistration",
                      "IDReferralIn",
                      "CodeScheme",
                      "Code",
                      "DateActivity",
                      "CodePosition",
                      "Site",
                      "Laterality",
                      "DateConfirmed",
                      "DateEnded",
                      "IDPatient",
                      "IDOrganisation"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getDateCreated() { return super.getCell("DateCreated");}
 public CsvCell getIdProfileCreatedBy() { return super.getCell("IdProfileCreatedBy");}
 public CsvCell getTypeOfEpisode() { return super.getCell("TypeOfEpisode");}
 public CsvCell getIDProfileConsultant() { return super.getCell("IDProfileConsultant");}
 public CsvCell getDateEpisodeStart() { return super.getCell("DateEpisodeStart");}
 public CsvCell getDateEpisodeEnd() { return super.getCell("DateEpisodeEnd");}
 public CsvCell getDateAuthorised() { return super.getCell("DateAuthorised");}
 public CsvCell getIDAppointment() { return super.getCell("IDAppointment");}
 public CsvCell getIDAttendance() { return super.getCell("IDAttendance");}
 public CsvCell getIDConsultantEvent() { return super.getCell("IDConsultantEvent");}
 public CsvCell getIDPatientRegistration() { return super.getCell("IDPatientRegistration");}
 public CsvCell getIDReferralIn() { return super.getCell("IDReferralIn");}
 public CsvCell getCodeScheme() { return super.getCell("CodeScheme");}
 public CsvCell getCode() { return super.getCell("Code");}
 public CsvCell getDateActivity() { return super.getCell("DateActivity");}
 public CsvCell getCodePosition() { return super.getCell("CodePosition");}
 public CsvCell getSite() { return super.getCell("Site");}
 public CsvCell getLaterality() { return super.getCell("Laterality");}
 public CsvCell getDateConfirmed() { return super.getCell("DateConfirmed");}
 public CsvCell getDateEnded() { return super.getCell("DateEnded");}
 public CsvCell getIDPatient() { return super.getCell("IDPatient");}
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");}


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRClinicalCode Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
