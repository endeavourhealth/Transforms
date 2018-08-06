package org.endeavourhealth.transform.tpp.csv.schema.unused.mentalhealth;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRSectionAppeal extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRSectionAppeal.class); 

  public SRSectionAppeal(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "AppealDate",
                      "AppealOutcome",
                      "AppealOutcomeDate",
                      "MedicalReportRequestDate",
                      "MedicalReportReceivedDate",
                      "NursingReportRequestDate",
                      "NursingReportReceivedDate",
                      "SocialCircumstanceRequestDate",
                      "SocialCircumstanceReceivedDate",
                      "SolicitorNoteReceivedDate",
                      "SolicitorNoteGrantedDate",
                      "IndependentReportGrantedDate",
                      "AppealType",
                      "DateCreation",
                      "IDProfileCreatedBy",
                      "IDSection",
                      "IDPatient",
                      "IDOrganisationDoneAt",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getAppealDate() { return super.getCell("AppealDate");}
 public CsvCell getAppealOutcome() { return super.getCell("AppealOutcome");}
 public CsvCell getAppealOutcomeDate() { return super.getCell("AppealOutcomeDate");}
 public CsvCell getMedicalReportRequestDate() { return super.getCell("MedicalReportRequestDate");}
 public CsvCell getMedicalReportReceivedDate() { return super.getCell("MedicalReportReceivedDate");}
 public CsvCell getNursingReportRequestDate() { return super.getCell("NursingReportRequestDate");}
 public CsvCell getNursingReportReceivedDate() { return super.getCell("NursingReportReceivedDate");}
 public CsvCell getSocialCircumstanceRequestDate() { return super.getCell("SocialCircumstanceRequestDate");}
 public CsvCell getSocialCircumstanceReceivedDate() { return super.getCell("SocialCircumstanceReceivedDate");}
 public CsvCell getSolicitorNoteReceivedDate() { return super.getCell("SolicitorNoteReceivedDate");}
 public CsvCell getSolicitorNoteGrantedDate() { return super.getCell("SolicitorNoteGrantedDate");}
 public CsvCell getIndependentReportGrantedDate() { return super.getCell("IndependentReportGrantedDate");}
 public CsvCell getAppealType() { return super.getCell("AppealType");}
 public CsvCell getDateCreation() { return super.getCell("DateCreation");}
 public CsvCell getIDProfileCreatedBy() { return super.getCell("IDProfileCreatedBy");}
 public CsvCell getIDSection() { return super.getCell("IDSection");}
 public CsvCell getIDPatient() { return super.getCell("IDPatient");}
 public CsvCell getIDOrganisationDoneAt() { return super.getCell("IDOrganisationDoneAt");}
 public CsvCell getRemovedData() { return super.getCell("RemovedData");}


 //fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRSectionAppeal Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
