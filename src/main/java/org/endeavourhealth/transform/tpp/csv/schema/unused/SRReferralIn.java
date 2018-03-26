package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRReferralIn extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRReferralIn.class); 

  public SRReferralIn(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "Category",
                      "PrimaryReason",
                      "DateReferral",
                      "DateOnReferralLetter",
                      "Source",
                      "ServiceOffered",
                      "ServiceType",
                      "ReReferral",
                      "Urgency",
                      "IDCodePrimaryDiagnosis",
                      "ReferralCode",
                      "IDReferralLocal",
                      "ReasonForServiceDelay",
                      "Outcome",
                      "DefaultContactLocation",
                      "DateDischargeScheduled",
                      "DateDischarge",
                      "DischargeReason",
                      "DischargeLocation",
                      "IDReferrer",
                      "SourceTableReferrer",
                      "PatientAware",
                      "DateOfAction",
                      "ActiveStatus",
                      "ReferredToSpeciality",
                      "ReferredToTreatmentCode",
                      "IDReferredTo",
                      "SourceTableReferrerTo",
                      "ReferralPriority",
                      "ReferralMedium",
                      "DateOfDecision",
                      "DayCareReferral",
                      "MaternityReferral",
                      "Commissioner",
                      "ClientCategory",
                      "PrimarySupportReason",
                      "IDCaseload",
                      "IDEvent",
                      "IDPatient",
                      "IDOrganisation",
                      "ReferralEndReason",
                      "HospitalReferralSourceCdsCode"
                    

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
 public CsvCell getCategory() { return super.getCell("Category");};
 public CsvCell getPrimaryReason() { return super.getCell("PrimaryReason");};
 public CsvCell getDateReferral() { return super.getCell("DateReferral");};
 public CsvCell getDateOnReferralLetter() { return super.getCell("DateOnReferralLetter");};
 public CsvCell getSource() { return super.getCell("Source");};
 public CsvCell getServiceOffered() { return super.getCell("ServiceOffered");};
 public CsvCell getServiceType() { return super.getCell("ServiceType");};
 public CsvCell getReReferral() { return super.getCell("ReReferral");};
 public CsvCell getUrgency() { return super.getCell("Urgency");};
 public CsvCell getIDCodePrimaryDiagnosis() { return super.getCell("IDCodePrimaryDiagnosis");};
 public CsvCell getReferralCode() { return super.getCell("ReferralCode");};
 public CsvCell getIDReferralLocal() { return super.getCell("IDReferralLocal");};
 public CsvCell getReasonForServiceDelay() { return super.getCell("ReasonForServiceDelay");};
 public CsvCell getOutcome() { return super.getCell("Outcome");};
 public CsvCell getDefaultContactLocation() { return super.getCell("DefaultContactLocation");};
 public CsvCell getDateDischargeScheduled() { return super.getCell("DateDischargeScheduled");};
 public CsvCell getDateDischarge() { return super.getCell("DateDischarge");};
 public CsvCell getDischargeReason() { return super.getCell("DischargeReason");};
 public CsvCell getDischargeLocation() { return super.getCell("DischargeLocation");};
 public CsvCell getIDReferrer() { return super.getCell("IDReferrer");};
 public CsvCell getSourceTableReferrer() { return super.getCell("SourceTableReferrer");};
 public CsvCell getPatientAware() { return super.getCell("PatientAware");};
 public CsvCell getDateOfAction() { return super.getCell("DateOfAction");};
 public CsvCell getActiveStatus() { return super.getCell("ActiveStatus");};
 public CsvCell getReferredToSpeciality() { return super.getCell("ReferredToSpeciality");};
 public CsvCell getReferredToTreatmentCode() { return super.getCell("ReferredToTreatmentCode");};
 public CsvCell getIDReferredTo() { return super.getCell("IDReferredTo");};
 public CsvCell getSourceTableReferrerTo() { return super.getCell("SourceTableReferrerTo");};
 public CsvCell getReferralPriority() { return super.getCell("ReferralPriority");};
 public CsvCell getReferralMedium() { return super.getCell("ReferralMedium");};
 public CsvCell getDateOfDecision() { return super.getCell("DateOfDecision");};
 public CsvCell getDayCareReferral() { return super.getCell("DayCareReferral");};
 public CsvCell getMaternityReferral() { return super.getCell("MaternityReferral");};
 public CsvCell getCommissioner() { return super.getCell("Commissioner");};
 public CsvCell getClientCategory() { return super.getCell("ClientCategory");};
 public CsvCell getPrimarySupportReason() { return super.getCell("PrimarySupportReason");};
 public CsvCell getIDCaseload() { return super.getCell("IDCaseload");};
 public CsvCell getIDEvent() { return super.getCell("IDEvent");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};
 public CsvCell getReferralEndReason() { return super.getCell("ReferralEndReason");};
 public CsvCell getHospitalReferralSourceCdsCode() { return super.getCell("HospitalReferralSourceCdsCode");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRReferralIn Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
