package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SROnAdmissionMedication extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SROnAdmissionMedication.class); 

  public SROnAdmissionMedication(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "NameOfMedication",
                      "DoseFrequency",
                      "DateMedicationStart",
                      "DateMedicationEnd",
                      "MedicationInformationSource",
                      "IDProfileMedicationInformationSource",
                      "MedicationEndReason",
                      "IDProfileMedicationEndedBy",
                      "Represcribed",
                      "IDProfileReprescribedBy",
                      "HistoryChecked",
                      "IDHistoryCheckedBy",
                      "IDProfileHistoryCheckedBy",
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
 public CsvCell getIDOrganisationDoneAt() { return super.getCell("IDOrganisationDoneAt");};
 public CsvCell getNameOfMedication() { return super.getCell("NameOfMedication");};
 public CsvCell getDoseFrequency() { return super.getCell("DoseFrequency");};
 public CsvCell getDateMedicationStart() { return super.getCell("DateMedicationStart");};
 public CsvCell getDateMedicationEnd() { return super.getCell("DateMedicationEnd");};
 public CsvCell getMedicationInformationSource() { return super.getCell("MedicationInformationSource");};
 public CsvCell getIDProfileMedicationInformationSource() { return super.getCell("IDProfileMedicationInformationSource");};
 public CsvCell getMedicationEndReason() { return super.getCell("MedicationEndReason");};
 public CsvCell getIDProfileMedicationEndedBy() { return super.getCell("IDProfileMedicationEndedBy");};
 public CsvCell getReprescribed() { return super.getCell("Represcribed");};
 public CsvCell getIDProfileReprescribedBy() { return super.getCell("IDProfileReprescribedBy");};
 public CsvCell getHistoryChecked() { return super.getCell("HistoryChecked");};
 public CsvCell getIDHistoryCheckedBy() { return super.getCell("IDHistoryCheckedBy");};
 public CsvCell getIDProfileHistoryCheckedBy() { return super.getCell("IDProfileHistoryCheckedBy");};
 public CsvCell getIDReferralIn() { return super.getCell("IDReferralIn");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};
 public CsvCell getIDEvent() { return super.getCell("IDEvent");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SROnAdmissionMedication Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
