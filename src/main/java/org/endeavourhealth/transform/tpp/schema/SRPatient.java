package org.endeavourhealth.transform.tpp.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TPPCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class SRPatient extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRPatient.class); 

  public SRPatient(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
            super(serviceId, systemId, exchangeId, version, filePath,
                    TPPCsvToFhirTransformer.CSV_FORMAT,
                    TPPCsvToFhirTransformer.DATE_FORMAT,
                    TPPCsvToFhirTransformer.TIME_FORMAT);
        }


        @Override
        protected String[] getCsvHeaders(String version) {
            return new String[]{
                      "RowIdentifier",
                      "IDOrganisationVisibleTo",
                      "Title",
                      "FirstName",
                      "MiddleNames",
                      "Surname",
                      "PreviousSurname",
                      "NHSNumber",
                      "DateBirth",
                      "DateDeath",
                      "BirthPlace",
                      "Gender",
                      "SpeaksEnglish",
                      "EmailAddress",
                      "TestPatient",
                      "SSRef",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getTitle() { return super.getCell("Title");};
 public CsvCell getFirstName() { return super.getCell("FirstName");};
 public CsvCell getMiddleNames() { return super.getCell("MiddleNames");};
 public CsvCell getSurname() { return super.getCell("Surname");};
 public CsvCell getPreviousSurname() { return super.getCell("PreviousSurname");};
 public CsvCell getNHSNumber() { return super.getCell("NHSNumber");};
 public CsvCell getDateBirth() { return super.getCell("DateBirth");};
 public CsvCell getDateDeath() { return super.getCell("DateDeath");};
 public CsvCell getBirthPlace() { return super.getCell("BirthPlace");};
 public CsvCell getGender() { return super.getCell("Gender");};
 public CsvCell getSpeaksEnglish() { return super.getCell("SpeaksEnglish");};
 public CsvCell getEmailAddress() { return super.getCell("EmailAddress");};
 public CsvCell getTestPatient() { return super.getCell("TestPatient");};
 public CsvCell getSSRef() { return super.getCell("SSRef");};
 public CsvCell getRemovedData() { return super.getCell("RemovedData");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRPatient Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
