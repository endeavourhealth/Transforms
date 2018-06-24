package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerNomenclatureRef;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.FamilyHistory;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.FamilyMemberHistoryBuilder;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.FamilyMemberHistory;
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
        //LOG.debug("Doing family history " + idCell.getString() + " for personID " + personIdCell.getString() + " with related person ID " + relatedPersonIdCell.getString());
        if (!BartsCsvHelper.isEmptyOrIsZero(relatedPersonIdCell)) {
            String relationshipType = csvHelper.getPatientRelationshipType(personIdCell, relatedPersonIdCell);
            //LOG.debug("Setting relationship type = " + relationshipType);
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
        }

        CsvCell nomenclatureId = parser.getActivityNomen();
        if (!BartsCsvHelper.isEmptyOrIsZero(nomenclatureId)) {

            CernerNomenclatureRef nomenclatureRef = csvHelper.lookupNomenclatureRef(nomenclatureId.getLong());
            String desc = nomenclatureRef.getDescriptionText();
            //Cerner DOES NOT use the Snomed family history codes, instead representing "FH: asthma"
            //by recording the asthma code in its family history table. If the SNOMED code is simply
            //carried through, then all analytics and record views will be wrong. So bring through
            //the term only and let the information-model map this later to a proper concept for FH.
            desc = "Family history: " + desc;

            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(familyMemberHistoryBuilder, CodeableConceptBuilder.Tag.Family_Member_History_Main_Code);
            codeableConceptBuilder.setText(desc);
        }

        //status is mandatory, so set the only possible status we can
        familyMemberHistoryBuilder.setStatus(FamilyMemberHistory.FamilyHistoryStatus.HEALTHUNKNOWN);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), familyMemberHistoryBuilder);
    }
}
