package org.endeavourhealth.transform.tpp.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TPPCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class SRPrimaryCareMedication extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRPrimaryCareMedication.class); 

  public SRPrimaryCareMedication(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "IDOrganisationDoneAt",
                      "IDMultiLexProduct",
                      "IDMultiLexPack",
                      "IDMultiLexDMD",
                      "NameOfMedication",
                      "DateMedicationStart",
                      "DateMedicationEnd",
                      "MedicationDosage",
                      "MedicationQuantity",
                      "IsRepeatMedication",
                      "IsOtherMedication",
                      "IsDentalMedication",
                      "IsHospitalMedication",
                      "IDRepeatTemplate",
                      "IDReferralIn",
                      "IDEvent",
                      "IDPatient",
                      "IDOrganisation"
                    

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
 public CsvCell getIDMultiLexProduct() { return super.getCell("IDMultiLexProduct");};
 public CsvCell getIDMultiLexPack() { return super.getCell("IDMultiLexPack");};
 public CsvCell getIDMultiLexDMD() { return super.getCell("IDMultiLexDMD");};
 public CsvCell getNameOfMedication() { return super.getCell("NameOfMedication");};
 public CsvCell getDateMedicationStart() { return super.getCell("DateMedicationStart");};
 public CsvCell getDateMedicationEnd() { return super.getCell("DateMedicationEnd");};
 public CsvCell getMedicationDosage() { return super.getCell("MedicationDosage");};
 public CsvCell getMedicationQuantity() { return super.getCell("MedicationQuantity");};
 public CsvCell getIsRepeatMedication() { return super.getCell("IsRepeatMedication");};
 public CsvCell getIsOtherMedication() { return super.getCell("IsOtherMedication");};
 public CsvCell getIsDentalMedication() { return super.getCell("IsDentalMedication");};
 public CsvCell getIsHospitalMedication() { return super.getCell("IsHospitalMedication");};
 public CsvCell getIDRepeatTemplate() { return super.getCell("IDRepeatTemplate");};
 public CsvCell getIDReferralIn() { return super.getCell("IDReferralIn");};
 public CsvCell getIDEvent() { return super.getCell("IDEvent");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRPrimaryCareMedication Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
