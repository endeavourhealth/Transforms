package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingDIAGNDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingDIAGN;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.DIAGN;
import org.endeavourhealth.transform.common.*;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.UUID;

public class DIAGNPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(DIAGNPreTransformer.class);

    private static StagingDIAGNDalI repository = DalProvider.factoryBartsStagingDIAGNDalI();

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        try {
            for (ParserI parser: parsers) {
                while (parser.nextRecord()) {
                    //no try/catch here, since any failure here means we don't want to continue
                    processRecord((DIAGN)parser, fhirResourceFiler, csvHelper);
                }
            }

        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    public static void processRecord(DIAGN parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        StagingDIAGN stagingDIAGN = new StagingDIAGN();

        stagingDIAGN.setExchangeId(parser.getExchangeId().toString());
        stagingDIAGN.setDtReceived(csvHelper.getDataDate());

        CsvCell diagnosisIdCell = parser.getDiagnosisID();
        stagingDIAGN.setDiagnosisId(diagnosisIdCell.getInt());

        boolean activeInd = parser.getActiveIndicator().getIntAsBoolean();
        stagingDIAGN.setActiveInd(activeInd);

        //only set additional values if active
        if (activeInd) {

            CsvCell encounterIdCell = parser.getEncounterId();
            stagingDIAGN.setEncounterId(encounterIdCell.getInt());

            String personId = csvHelper.findPersonIdFromEncounterId(encounterIdCell);
            if (Strings.isNullOrEmpty(personId)) {
                TransformWarnings.log(LOG, csvHelper, "No person ID found for DIAGN {}", diagnosisIdCell);
                return;
            }

            if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
                TransformWarnings.log(LOG, csvHelper, "Skipping DIAGN {} as not part of filtered subset", diagnosisIdCell);
                return;
            }

            stagingDIAGN.setLookupPersonId(Integer.valueOf(personId));

            //TYPE_MILLENNIUM_PERSON_ID_TO_MRN
            String mrn = csvHelper.getInternalId(InternalIdMap.TYPE_MILLENNIUM_PERSON_ID_TO_MRN, personId);
            if (mrn != null) {
                stagingDIAGN.setLookupMrn(mrn);
            } else {
                TransformWarnings.log(LOG, csvHelper, "DIAGN {} has no MRN from lookup for person {}", diagnosisIdCell, personId);
            }

            CsvCell responsiblePersonnelId = parser.getPersonnelId();
            if (!responsiblePersonnelId.isEmpty()) {
                stagingDIAGN.setDiagnosisPersonnelId(responsiblePersonnelId.getInt());
            }

            CsvCell sliceCell = parser.getEncounterSliceID();
            if (!sliceCell.isEmpty()) {
                stagingDIAGN.setEncounterSliceId(sliceCell.getInt());
            }

            CsvCell dateCell = parser.getDiagnosisDateTime();
            Date diagnosisDate = BartsCsvHelper.parseDate(dateCell);
            //explicitly been told that if a DIAGN record has no date, then skip it
            if (diagnosisDate == null) {
                //LOG.debug("Skipping this diagnosis with no date");
                return;
            }
            stagingDIAGN.setDiagnosisDtTm(diagnosisDate);

            CsvCell conceptCell = parser.getConceptCodeIdentifier();
            String codeId = csvHelper.getProcedureOrDiagnosisConceptCode(conceptCell);
            if (Strings.isNullOrEmpty(codeId)) {
                //a very small number of DIAGN records have no code but we can't do anything with it
                TransformWarnings.log(LOG, csvHelper, "Diagnosis {} has no concept code so will be skipped", diagnosisIdCell);
                return;
            }
            stagingDIAGN.setDiagnosisCode(codeId);

            String codeType = csvHelper.getProcedureOrDiagnosisConceptCodeType(parser.getConceptCodeIdentifier());
            stagingDIAGN.setDiagnosisCodeType(codeType);

            String diagnosisTerm;
            if (codeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_ICD_10)) {
                diagnosisTerm = TerminologyService.lookupIcd10CodeDescription(codeId);
                if (Strings.isNullOrEmpty(diagnosisTerm)) {
                    throw new Exception("Failed to find term for ICD10 code " + codeId);
                }
            } else if (codeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_SNOMED)) {
                diagnosisTerm = TerminologyService.lookupSnomedTerm(codeId);
                if (Strings.isNullOrEmpty(diagnosisTerm)) {
                    throw new Exception("Failed to find term for Snomed code " + codeId);
                }
            } else if (codeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_OPCS_4)) {
                diagnosisTerm = TerminologyService.lookupOpcs4ProcedureName(codeId);
                if (Strings.isNullOrEmpty(diagnosisTerm)) {
                    throw new Exception("Failed to find term for OPCS-4 code " + codeId);
                }
            } else if (codeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_HRG)) {
                TransformWarnings.log(LOG, csvHelper, "PROCE record {} has HRG code in concept cell {}", diagnosisIdCell, conceptCell);
                return;

            } else {
                throw new Exception("Unexpected code type " + codeType);
            }
            stagingDIAGN.setDiagnosisTerm(diagnosisTerm);

            CsvCell sequenceNumberCell = parser.getSequenceNumber();
            //there are a small number of DIAGN records with a zero sequence number, so ignore it
            if (!BartsCsvHelper.isEmptyOrIsZero(sequenceNumberCell)) {
                stagingDIAGN.setDiagnosisSeqNo(sequenceNumberCell.getInt());
            }

            CsvCell diagnosisTypeCode = parser.getDiagnosisTypeCode();
            if (!diagnosisTypeCode.isEmpty()) {
                stagingDIAGN.setDiagnosisType(diagnosisTypeCode.getString());
            }

            CsvCell diagnosisNotes = parser.getDiagnosisFreeText();
            if (!diagnosisNotes.isEmpty()) {
                stagingDIAGN.setDiagnosisNotes(diagnosisNotes.getString());
            }
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new DIAGNPreTransformer.saveDataCallable(parser.getCurrentState(), stagingDIAGN, serviceId));

        //TODO: revist this when Encounters work recommences
//        PreTransformCallable callable = new PreTransformEncounterLinkCallable(parser.getCurrentState(), diagnosisIdCell, encounterIdCell, csvHelper);
//        csvHelper.submitToThreadPool(callable);
    }


    private static class saveDataCallable extends AbstractCsvCallable {

        private StagingDIAGN obj = null;
        private UUID serviceId;

        public saveDataCallable(CsvCurrentState parserState,
                                StagingDIAGN obj,
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

    //TODO: revist this when Encounters work recommences
    static class PreTransformEncounterLinkCallable extends AbstractCsvCallable {

        private CsvCell diagnosisIdCell;
        private CsvCell encounterIdCell;
        private BartsCsvHelper csvHelper;

        public PreTransformEncounterLinkCallable(CsvCurrentState parserState, CsvCell diagnosisIdCell, CsvCell encounterIdCell, BartsCsvHelper csvHelper) {
            super(parserState);
            this.diagnosisIdCell = diagnosisIdCell;
            this.encounterIdCell = encounterIdCell;
            this.csvHelper = csvHelper;
        }


        @Override
        public Object call() throws Exception {

            try {
                csvHelper.cacheNewConsultationChildRelationship(encounterIdCell, diagnosisIdCell, ResourceType.Condition);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }

}
