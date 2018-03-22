package org.endeavourhealth.transform.tpp.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TPPCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class SROutPatientMedication extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SROutPatientMedication.class); 

  public SROutPatientMedication(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "DateEventRecorded",
                      "DateEvent",
                      "IDProfileEnteredBy",
                      "IDDoneBy",
                      "TextualEventDoneBy",
                      "NameOfMedication",
                      "StandardOrInstalment",
                      "StandardIssueDose",
                      "StandardIssueQuantity",
                      "InstalmentDailyDose",
                      "InstalmentSupervisedDose",
                      "DateFirstAdmin",
                      "DateSupplyUntil",
                      "Continuation",
                      "SourceOfSupply",
                      "DateMedicationEnd",
                      "MedicationEndReason",
                      "IDMedicationEndedBy",
                      "IDProfileMedicationEndedBy",
                      "IDReferralIn",
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
 public CsvCell getNameOfMedication() { return super.getCell("NameOfMedication");};
 public CsvCell getStandardOrInstalment() { return super.getCell("StandardOrInstalment");};
 public CsvCell getStandardIssueDose() { return super.getCell("StandardIssueDose");};
 public CsvCell getStandardIssueQuantity() { return super.getCell("StandardIssueQuantity");};
 public CsvCell getInstalmentDailyDose() { return super.getCell("InstalmentDailyDose");};
 public CsvCell getInstalmentSupervisedDose() { return super.getCell("InstalmentSupervisedDose");};
 public CsvCell getDateFirstAdmin() { return super.getCell("DateFirstAdmin");};
 public CsvCell getDateSupplyUntil() { return super.getCell("DateSupplyUntil");};
 public CsvCell getContinuation() { return super.getCell("Continuation");};
 public CsvCell getSourceOfSupply() { return super.getCell("SourceOfSupply");};
 public CsvCell getDateMedicationEnd() { return super.getCell("DateMedicationEnd");};
 public CsvCell getMedicationEndReason() { return super.getCell("MedicationEndReason");};
 public CsvCell getIDMedicationEndedBy() { return super.getCell("IDMedicationEndedBy");};
 public CsvCell getIDProfileMedicationEndedBy() { return super.getCell("IDProfileMedicationEndedBy");};
 public CsvCell getIDReferralIn() { return super.getCell("IDReferralIn");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};
 public CsvCell getIDEvent() { return super.getCell("IDEvent");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SROutPatientMedication Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
