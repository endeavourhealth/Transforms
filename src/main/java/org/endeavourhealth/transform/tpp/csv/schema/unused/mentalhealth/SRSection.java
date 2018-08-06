package org.endeavourhealth.transform.tpp.csv.schema.unused.mentalhealth;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRSection extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRSection.class); 

  public SRSection(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "SectionStartDate",
                      "SectionExpiryDate",
                      "SectionType",
                      "SectionCategory",
                      "ConsentDueDate",
                      "ResponsibleClinician",
                      "SectionOutcome",
                      "MHRTReferralDue",
                      "SectionEndDate",
                      "ResponsibleCCG",
                      "ResponsibleTrust",
                      "DateCreation",
                      "IDProfileCreatedBy",
                      "IDEvent",
                      "IDPatient",
                      "IDOrganisation",
                      "DateClinicianBecameResponsible",
                      "Active"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getSectionStartDate() { return super.getCell("SectionStartDate");}
 public CsvCell getSectionExpiryDate() { return super.getCell("SectionExpiryDate");}
 public CsvCell getSectionType() { return super.getCell("SectionType");}
 public CsvCell getSectionCategory() { return super.getCell("SectionCategory");}
 public CsvCell getConsentDueDate() { return super.getCell("ConsentDueDate");}
 public CsvCell getResponsibleClinician() { return super.getCell("ResponsibleClinician");}
 public CsvCell getSectionOutcome() { return super.getCell("SectionOutcome");}
 public CsvCell getMHRTReferralDue() { return super.getCell("MHRTReferralDue");}
 public CsvCell getSectionEndDate() { return super.getCell("SectionEndDate");}
 public CsvCell getResponsibleCCG() { return super.getCell("ResponsibleCCG");}
 public CsvCell getResponsibleTrust() { return super.getCell("ResponsibleTrust");}
 public CsvCell getDateCreation() { return super.getCell("DateCreation");}
 public CsvCell getIDProfileCreatedBy() { return super.getCell("IDProfileCreatedBy");}
 public CsvCell getIDEvent() { return super.getCell("IDEvent");}
 public CsvCell getIDPatient() { return super.getCell("IDPatient");}
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");}
 public CsvCell getDateClinicianBecameResponsible() { return super.getCell("DateClinicianBecameResponsible");}
 public CsvCell getActive() { return super.getCell("Active");}


 //fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRSection Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
