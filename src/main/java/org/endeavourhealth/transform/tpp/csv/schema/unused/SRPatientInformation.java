package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRPatientInformation extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRPatientInformation.class); 

  public SRPatientInformation(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
            super(serviceId, systemId, exchangeId, version, filePath,
                    TppCsvToFhirTransformer.CSV_FORMAT,
                    TppCsvToFhirTransformer.DATE_FORMAT,
                    TppCsvToFhirTransformer.TIME_FORMAT,
                    TppCsvToFhirTransformer.ENCODING);
        }

    //
    //
    //  ====>    Deprecated   <========
    //
    //
    //

        @Override
        protected String[] getCsvHeaders(String version) {
            if (version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK_2)) {
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
                        "PreferredPharmacy",
                        "TestPatient",
                        "DateRegistration",
                        "DateDeRegistration",
                        "RegistrationStatus",
                        "SSRef",
                        "IDPatient",
                        "IDOrganisation"
                };
            } else {
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
                        "PreferredPharmacy",
                        "TestPatient",
                        "DateRegistration",
                        "DateDeRegistration",
                        "RegistrationStatus",
                        "SSRef",
                        "IDPatient",
                        "IDOrganisation",
                        "RemovedData"
                };
            }
        }

 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getTitle() { return super.getCell("Title");}
 public CsvCell getFirstName() { return super.getCell("FirstName");}
 public CsvCell getMiddleNames() { return super.getCell("MiddleNames");}
 public CsvCell getSurname() { return super.getCell("Surname");}
 public CsvCell getPreviousSurname() { return super.getCell("PreviousSurname");}
 public CsvCell getNHSNumber() { return super.getCell("NHSNumber");}
 public CsvCell getDateBirth() { return super.getCell("DateBirth");}
 public CsvCell getDateDeath() { return super.getCell("DateDeath");}
 public CsvCell getBirthPlace() { return super.getCell("BirthPlace");}
 public CsvCell getGender() { return super.getCell("Gender");}
 public CsvCell getSpeaksEnglish() { return super.getCell("SpeaksEnglish");}
 public CsvCell getEmailAddress() { return super.getCell("EmailAddress");}
 public CsvCell getPreferredPharmacy() { return super.getCell("PreferredPharmacy");}
 public CsvCell getTestPatient() { return super.getCell("TestPatient");}
 public CsvCell getDateRegistration() { return super.getCell("DateRegistration");}
 public CsvCell getDateDeRegistration() { return super.getCell("DateDeRegistration");}
 public CsvCell getRegistrationStatus() { return super.getCell("RegistrationStatus");}
 public CsvCell getSSRef() { return super.getCell("SSRef");}
 public CsvCell getIDPatient() { return super.getCell("IDPatient");}
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");}
 public CsvCell getRemovedData() { return super.getCell("RemovedData");}


 //fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRPatientInformation Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
