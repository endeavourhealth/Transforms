package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingDiagnosisDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingDiagnosis;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.terminology.SnomedCode;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public class DiagnosisPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(DiagnosisPreTransformer.class);

    private static StagingDiagnosisDalI repository = DalProvider.factoryBartsStagingDiagnosisDalI();


    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser : parsers) {

            while (parser.nextRecord()) {
                //no try/catch here, since any failure here means we don't want to continue
                    processRecord((org.endeavourhealth.transform.barts.schema.Diagnosis) parser, csvHelper);
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void processRecord(org.endeavourhealth.transform.barts.schema.Diagnosis parser, BartsCsvHelper csvHelper) throws Exception {

        CsvCell diagnosisIdCell = parser.getDiagnosisIdSanitised();

        CsvCell personIdCell = parser.getPersonIdIdSanitised();
        if (personIdCell.isEmpty()) {
            TransformWarnings.log(LOG, csvHelper, "No person ID found for Diagnosis for diagnosis ID {}", diagnosisIdCell);
            return;
        }

        if (!csvHelper.processRecordFilteringOnPatientId(personIdCell.getString())) {
            TransformWarnings.log(LOG, csvHelper, "Skipping Diagnosis with Diagnosis ID {} as not part of filtered subset", diagnosisIdCell);
            return;
        }

        StagingDiagnosis obj = new StagingDiagnosis();
        obj.setExchangeId(parser.getExchangeId().toString());
        obj.setDtReceived(csvHelper.getDataDate());
        obj.setDiagnosisId(diagnosisIdCell.getInt());

        //audit that our staging object came from this file and record
        ResourceFieldMappingAudit audit = new ResourceFieldMappingAudit();
        audit.auditRecord(diagnosisIdCell.getPublishedFileId(), diagnosisIdCell.getRecordNumber());
        obj.setAudit(audit);

        //NOTE: all data columns are available when active_ind = 1 or 0
        boolean activeInd = parser.getActiveIndicator().getIntAsBoolean();
        obj.setActiveInd(activeInd);

        CsvCell encounterCell = parser.getEncounterIdSanitised();
        obj.setEncounterId(encounterCell.getInt());

        obj.setPersonId(personIdCell.getInt());
        obj.setMrn(parser.getMRN().getString());

        CsvCell dtCell = parser.getDiagnosisDate();
        obj.setDiagDtTm(BartsCsvHelper.parseDate(dtCell));


        CsvCell diagnosisTypeCell = parser.getDiagType();
        obj.setDiagType(diagnosisTypeCell.getString());

        CsvCell diagnosisConsultant = parser.getDiagPrnsl();
        obj.setConsultant(diagnosisConsultant.getString());

        CsvCell vocabCell = parser.getVocabulary();
        obj.setVocab(vocabCell.getString());

        CsvCell diagCodeCell = parser.getDiagnosisCode();
        String diagCode = diagCodeCell.getString();
        String diagTerm = "";
        if (vocabCell.getString().equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_OPCS_4)) {

            diagTerm = TerminologyService.lookupOpcs4ProcedureName(diagCode);
            if (Strings.isNullOrEmpty(diagTerm)) {
                throw new Exception("Failed to find term for OPCS-4 code [" + diagCode + "]");
            }

        } else if (vocabCell.getString().equals(BartsCsvHelper.CODE_TYPE_SNOMED_CT) ||
                    vocabCell.getString().equals(BartsCsvHelper.CODE_TYPE_UK_ED_SUBSET)) {
            // note, although the column says it's Snomed or UK Ed Subset,
            // these are actually a Snomed description ID, not a concept ID
            SnomedCode snomedCode = TerminologyService.lookupSnomedConceptForDescriptionId(diagCode);
            if (snomedCode == null) {
                throw new Exception("Failed to find term for Snomed description ID [" + diagCode + "]");
            }
            diagTerm = snomedCode.getTerm();
            diagCode = snomedCode.getConceptCode();  //update the code to be an actual Snomed ConceptId

        } else if (vocabCell.getString().trim().equals("[Allergy]")) {
            TransformWarnings.log(LOG,csvHelper,"Allergycoding.  Found Allergy as vocab for diagnosis id: {}", diagnosisIdCell.getString() );
            return;
        } else {
            throw new Exception("Unexpected coding scheme vocab" + vocabCell.getString());
        }

        obj.setDiagCd(diagCode);
        obj.setDiagTerm(diagTerm);

        CsvCell classificationCell = parser.getClassification();
        if (!classificationCell.isEmpty()) {
            obj.setClassification(classificationCell.getString());
        }

        CsvCell rankCell = parser.getRank();
        if (!rankCell.isEmpty()) {
            obj.setRanking(rankCell.getString());
        }

        CsvCell axisCell = parser.getAxis();
        if (!axisCell.isEmpty()) {
            obj.setAxis(axisCell.getString());
        }

        CsvCell confirmationCell = parser.getConfirmation();
        if (!confirmationCell.isEmpty()) {
            obj.setConfirmation(confirmationCell.getString());
        }

        //free text notes built up from numerous text items
        obj.setNotes(buildDiagnosisNotes(parser));

        CsvCell locationCell = parser.getOrgName();
        if (!locationCell.isEmpty()) {
            obj.setLocation(locationCell.getString());
        }

        String consultantPersonnelId = csvHelper.getInternalId(PRSNLREFTransformer.MAPPING_ID_PERSONNEL_NAME_TO_ID, diagnosisConsultant.getString());
        if (!Strings.isNullOrEmpty(consultantPersonnelId)) {
            obj.setLookupConsultantPersonnelId(Integer.valueOf(consultantPersonnelId));
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new DiagnosisPreTransformer.saveDataCallable(parser.getCurrentState(), obj, serviceId));
    }

    private static class saveDataCallable extends AbstractCsvCallable {

        private StagingDiagnosis obj = null;
        private UUID serviceId;

        public saveDataCallable(CsvCurrentState parserState,
                                StagingDiagnosis obj,
                                UUID serviceId) {
            super(parserState);
            this.obj = obj;
            this.serviceId = serviceId;
        }

        @Override
        public Object call() throws Exception {

            try {
                repository.save(obj, serviceId);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }

    private static String buildDiagnosisNotes (org.endeavourhealth.transform.barts.schema.Diagnosis parser) {

        //collect up all the free text elements: classification, rank, axis, severity, certainty and secondary descriptions'
        StringBuilder notes = new StringBuilder();

        CsvCell diagnosisCell = parser.getDiagnosis();
        if (!diagnosisCell.isEmpty()) {
         notes.append("Diagnosis: " + diagnosisCell.getString()+". ");
        }
        CsvCell clinicalService = parser.getClinService();
        if (!clinicalService.isEmpty()) {
            notes.append("ClinService: "+ clinicalService.getString()+". ");
        }
        CsvCell qualifier = parser.getQualifier();
        if (!qualifier.isEmpty()) {
            notes.append("Qualifier: "+qualifier.getString()+". ");
        }
        CsvCell severity = parser.getSeverity();
        if (!severity.isEmpty()) {
            notes.append("Severity: "+severity.getString()+". ");
        }
        CsvCell severityClass = parser.getSeverityClass();
        if (!severityClass.isEmpty()) {
            notes.append("SeverityClass: "+severityClass.getString()+". ");
        }
        CsvCell certainty = parser.getCertainty();
        if (!certainty.isEmpty()) {
            notes.append("Certainty: "+certainty.getString()+". ");
        }
        CsvCell probability = parser.getProbability();
        if (!probability.isEmpty()) {
            notes.append("Probability: "+ probability.getString()+". ");
        }
        CsvCell secondaryDescription = parser.getSecondaryDescription();
        if (!secondaryDescription.isEmpty()) {
            notes.append("Secondary Description: "+secondaryDescription.getString()+". ");
        }

        return notes.toString();
    }
}
