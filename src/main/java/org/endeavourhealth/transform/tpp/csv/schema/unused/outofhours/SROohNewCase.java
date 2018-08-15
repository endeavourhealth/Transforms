package org.endeavourhealth.transform.tpp.csv.schema.unused.outofhours;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SROohNewCase extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SROohNewCase.class); 

  public SROohNewCase(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
            super(serviceId, systemId, exchangeId, version, filePath,
                    TppCsvToFhirTransformer.CSV_FORMAT,
                    TppCsvToFhirTransformer.DATE_FORMAT,
                    TppCsvToFhirTransformer.TIME_FORMAT,
                    TppCsvToFhirTransformer.ENCODING);
        }


        @Override
        protected String[] getCsvHeaders(String version) {
            return new String[]{
                      "RowIdentifier",
                      "IDOrganisationVisibleTo",
                      "CallOriginCallCentre",
                      "CallStreamAE",
                      "CallStreamMIU",
                      "CategoryC",
                      "DateCreated",
                      "IDProfileEnteredBy",
                      "CurrentNameOfBuilding",
                      "CurrentNumberOfBuilding",
                      "CurrentNameOfRoad",
                      "CurrentNameOfLocality",
                      "CurrentNameOfTown",
                      "CurrentFullPostcode",
                      "CaseType",
                      "DateStarted",
                      "DateWalkedIn",
                      "PatientCall",
                      "PatientCallBehalfContact",
                      "PatientCallInformantContact",
                      "PatientCallInformantContactExt",
                      "ContactNumber",
                      "ContactNumberExt",
                      "SecondContactNumber",
                      "SecondContactNumberExt",
                      "DateInterpreterRequired",
                      "DateInterpreterProvided",
                      "InterpreterLanguage",
                      "DateClosed",
                      "ClosedCoreActivity",
                      "CloseCaseFollowUp",
                      "IDPatient",
                      "IDOrganisation"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getCallOriginCallCentre() { return super.getCell("CallOriginCallCentre");}
 public CsvCell getCallStreamAE() { return super.getCell("CallStreamAE");}
 public CsvCell getCallStreamMIU() { return super.getCell("CallStreamMIU");}
 public CsvCell getCategoryC() { return super.getCell("CategoryC");}
 public CsvCell getDateCreated() { return super.getCell("DateCreated");}
 public CsvCell getIDProfileEnteredBy() { return super.getCell("IDProfileEnteredBy");}
 public CsvCell getCurrentNameOfBuilding() { return super.getCell("CurrentNameOfBuilding");}
 public CsvCell getCurrentNumberOfBuilding() { return super.getCell("CurrentNumberOfBuilding");}
 public CsvCell getCurrentNameOfRoad() { return super.getCell("CurrentNameOfRoad");}
 public CsvCell getCurrentNameOfLocality() { return super.getCell("CurrentNameOfLocality");}
 public CsvCell getCurrentNameOfTown() { return super.getCell("CurrentNameOfTown");}
 public CsvCell getCurrentFullPostcode() { return super.getCell("CurrentFullPostcode");}
 public CsvCell getCaseType() { return super.getCell("CaseType");}
 public CsvCell getDateStarted() { return super.getCell("DateStarted");}
 public CsvCell getDateWalkedIn() { return super.getCell("DateWalkedIn");}
 public CsvCell getPatientCall() { return super.getCell("PatientCall");}
 public CsvCell getPatientCallBehalfContact() { return super.getCell("PatientCallBehalfContact");}
 public CsvCell getPatientCallInformantContact() { return super.getCell("PatientCallInformantContact");}
 public CsvCell getPatientCallInformantContactExt() { return super.getCell("PatientCallInformantContactExt");}
 public CsvCell getContactNumber() { return super.getCell("ContactNumber");}
 public CsvCell getContactNumberExt() { return super.getCell("ContactNumberExt");}
 public CsvCell getSecondContactNumber() { return super.getCell("SecondContactNumber");}
 public CsvCell getSecondContactNumberExt() { return super.getCell("SecondContactNumberExt");}
 public CsvCell getDateInterpreterRequired() { return super.getCell("DateInterpreterRequired");}
 public CsvCell getDateInterpreterProvided() { return super.getCell("DateInterpreterProvided");}
 public CsvCell getInterpreterLanguage() { return super.getCell("InterpreterLanguage");}
 public CsvCell getDateClosed() { return super.getCell("DateClosed");}
 public CsvCell getClosedCoreActivity() { return super.getCell("ClosedCoreActivity");}
 public CsvCell getCloseCaseFollowUp() { return super.getCell("CloseCaseFollowUp");}
 public CsvCell getIDPatient() { return super.getCell("IDPatient");}
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");}


 //fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SROohNewCase Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
