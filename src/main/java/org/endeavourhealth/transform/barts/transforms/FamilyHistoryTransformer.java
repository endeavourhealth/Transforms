package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.FamilyHistory;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.FamilyMemberHistoryBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.FamilyMemberHistory;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class FamilyHistoryTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(FamilyHistoryTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    createResource((FamilyHistory)parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    private static void createResource(FamilyHistory parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        /**
            IGNORE getFhxActivityKey() {
            IGNORE getHealthSystemId() {
            IGNORE  getHealthSystemSourceId() {
            DONE getFhxActivityId() {
            IGNORE getFhxActivityGroupId() {
            DONE getPersonId() {
            DONE getRelatedPersonId() {
            DONE getRelatedPersonReltnRef() {
            IGNORE getFhxvalueFlag()
            IGNORE getOnsetAge() {
            IGNORE getOnsetAgrePrecRef() {
            IGNORE getOnsetAgeUnitRef() {
            IGNORE getActivityOrg() {
            IGNORE getCourseRef() {
            IGNORE getLifeCycleStatusRef() {
            DONE getActivityNomen() {
            IGNORE getSeverityRef() {
            IGNORE getTypeMean() {
            DONE getSrcBegEffectDtTm() {
            IGNORE getSrcBegEffectTmVldFlg() {
            IGNORE getSrcBegEffectTmZn() {
            DONE getSrcEndEffectDtTm() {
            IGNORE getSrcEndEffectTmVldFlg() {
            IGNORE getSrcEndEffectTmZn() {
            DONE getActiveInd() {
            IGNORE getTranPrsnlHssId() {
            DONE getCreatePrsnl() {
            IGNORE getCreateTransPrsnl() {
            DONE getCreateDtTm() {
            IGNORE getCreateTmVldFlg() {
            IGNORE getCreateTmZn() {
            IGNORE getInactivatePrsnl() {
             IGNORE getInactivateTranPrsl() {
             IGNORE getInactivateDtTm() {
             IGNORE getInactivateTmVldFlg() {
            IGNORE getInactivateTmZn() {
             IGNORE getFirstReviewPrsnl() {
             IGNORE getFirstReviewTranPrsnl() {
             IGNORE getFirstReviewDtTM() {
             IGNORE getFirstReviewTmVldFlg() {
            IGNORE getFirstReviewTmZn() {
             IGNORE getLastReviewPrsnl() {
             IGNORE getLastReviewTranPrsnl() {
             IGNORE getLastReviewDtTM() {
             IGNORE getLastReviewTmVldFlg() {
            IGNORE getLastReviewTmZn() {
            IGNORE getSecurityKey() {
            IGNORE getSecurityProcessDtTm() {
            IGNORE getDuplicateFlag() {
            IGNORE getOrphanFlag() {
            IGNORE getErrorInd() {
             IGNORE getFirstProcessDtTm() {
             IGNORE getLastProcessDtTm() {
             IGNORE getTotalUpdates() {
             IGNORE getUpdtDtTm() {
             IGNORE getUpdtTask() {
             IGNORE getUpdtUser() {
            IGNORE getSourceFlag() {
            IGNORE getExtractDtTm() {
            IGNORE getPartitionDtTm() {
            IGNORE getRecordUpdatedDt() {

         want to set
            DONE ID
            DONE patient
            DONE date
            DONE status
            DONE relationship
         code //TODO - look up nomenclature
            NO NOTES notes
            CANT DO clinician
            CANT DO encounter
            DONE recorded by
            DONE recorded date
         */
        
        FamilyMemberHistoryBuilder familyMemberHistoryBuilder = new FamilyMemberHistoryBuilder();

        CsvCell idCell = parser.getFhxActivityKey();
        familyMemberHistoryBuilder.setId(idCell.getString(), idCell);

        CsvCell personIdCell = parser.getPersonId();
        Reference patientReference = csvHelper.createPatientReference(personIdCell);
        familyMemberHistoryBuilder.setPatient(patientReference, personIdCell);

        CsvCell activeCell = parser.getActiveInd();
        if (!activeCell.getIntAsBoolean()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), familyMemberHistoryBuilder);
            return;
        }

        //related person links to the PPREL table, the contents of which end up
        //as PatientContact elements on the Patient resource
        CsvCell relatedPersonIdCell = parser.getRelatedPersonId();
        if (!BartsCsvHelper.isEmptyOrIsZero(relatedPersonIdCell)) {

            String relationshipType = csvHelper.getPatientRelationshipType(relatedPersonIdCell, personIdCell);
            familyMemberHistoryBuilder.setRelationshipFreeText(relationshipType, relatedPersonIdCell);
        }

        CsvCell beginDateCell = parser.getSrcBegEffectDtTm();
        if (!BartsCsvHelper.isEmptyOrIsStartOfTime(beginDateCell)) {
            Date d = BartsCsvHelper.parseDate(beginDateCell);
            DateTimeType dateTimeType = new DateTimeType(d);
            familyMemberHistoryBuilder.setDate(dateTimeType, beginDateCell);
        }

        CsvCell endDateCell = parser.getSrcEndEffectDtTm();
        if (!BartsCsvHelper.isEmptyOrIsEndOfTime(endDateCell)) {
            Date d = BartsCsvHelper.parseDate(endDateCell);
            DateTimeType dateTimeType = new DateTimeType(d);
            familyMemberHistoryBuilder.setEndDate(dateTimeType, endDateCell);
        }

        CsvCell createdDateCell = parser.getCreateDtTm();
        if (!BartsCsvHelper.isEmptyOrIsEndOfTime(createdDateCell)) {
            Date d = BartsCsvHelper.parseDate(createdDateCell);
            familyMemberHistoryBuilder.setRecordedDate(d, createdDateCell);
        }

        CsvCell createdByCell = parser.getCreatePrsnl();
        if (!BartsCsvHelper.isEmptyOrIsZero(createdByCell)) {
            Reference practitionerReference = csvHelper.createPractitionerReference(createdByCell);
            familyMemberHistoryBuilder.setRecordedBy(practitionerReference, createdByCell);
            //TODO - validate that this does link to PERSONREF
        }

        CsvCell nomenclatureId = parser.getActivityNomen();
        if (!BartsCsvHelper.isEmptyOrIsZero(nomenclatureId)) {

            //TODO - I think we need a bulk of the NOMREF table and need to look up to it here

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(familyMemberHistoryBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_NOMENCLATURE_ID);
            identifierBuilder.setValue(nomenclatureId.getString(), nomenclatureId);
        }

        //status is mandatory, so set the only possible status we can
        familyMemberHistoryBuilder.setStatus(FamilyMemberHistory.FamilyHistoryStatus.HEALTHUNKNOWN);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), familyMemberHistoryBuilder);
    }
}
