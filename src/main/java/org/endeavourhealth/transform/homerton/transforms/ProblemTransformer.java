package org.endeavourhealth.transform.homerton.transforms;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.endeavourhealth.transform.homerton.HomertonCsvHelper;
import org.endeavourhealth.transform.homerton.schema.ProblemTable;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class ProblemTransformer extends HomertonBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ProblemTransformer.class);

    public static void transform(String version,
                                 List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonCsvHelper csvHelper,
                                 String primaryOrgOdsCode) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    String valStr = validateEntry((ProblemTable) parser);
                    if (valStr == null) {
                        createCondition((ProblemTable) parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode);
                    } else {
                        TransformWarnings.log(LOG, parser, "Validation error: {}", valStr);
                    }
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    /*
     *
     */
    public static String validateEntry(ProblemTable parser) {
        return null;
    }

    public static void createCondition(ProblemTable parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       HomertonCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode) throws Exception {
        CodeableConcept cc = null;
        Date d = null;

        // Organisation - Since EpisodeOfCare record is not established no need for Organization either
        // PatientTable
        //ResourceId patientResourceId = resolvePatientResource(parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getLocalPatientId(), null, null, null, null, null, null, null);
        // EpisodeOfCare - ProblemTable record cannot be linked to an EpisodeOfCare
        // EncounterTable - ProblemTable record cannot be linked to an EncounterTable
        // this ProblemTable resource id
        //ResourceId problemResourceId = getProblemResourceId(parser.getLocalPatientId(), parser.getOnsetDateAsString(), parser.getProblemCode());

        //CodeableConcept problemCode = new CodeableConcept();
        //problemCode.addCoding().setCode(parser.getProblemCode()).setSystem(getCodeSystemName(HomertonCsvToFhirTransformer.CODE_SYSTEM_SNOMED)).setDisplay(parser.getProblem());

        //Identifiers
        //Identifier identifiers[] = {new Identifier().setSystem(HomertonCsvToFhirTransformer.CODE_SYSTEM_PROBLEM_ID).setValue(parser.getProblemId().toString())};

        //DateTimeType onsetDate = new DateTimeType(parser.getOnsetDate());

        ConditionBuilder conditionBuilder = new ConditionBuilder();
        //createConditionResource(fhirCondition, problemResourceId, patientResourceId, null, parser.getUpdateDateTime(), problemCode, onsetDate, parser.getAnnotatedDisp(), identifiers);

        //ResourceId patientResourceId = resolvePatientResource(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, null, parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getCNN(), parser.getNHSNo(), name, fhirAddress, convertSusGenderToFHIR(parser.getGender()), parser.getDOB(), organisationResourceId, null, patientIdentifier, gpResourceId, gpPracticeResourceId, ethnicGroup);

        //ReferenceHelper.createReference(ResourceType.PatientTable, patientResourceId.getResourceId().toString()));

        CsvCell problemIdCell = parser.getProblemId();
        conditionBuilder.setId(problemIdCell.getString(), problemIdCell);

        // set patient reference
        CsvCell personIdCell = parser.getPersonId();
        Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, personIdCell.getString());
        conditionBuilder.setPatient(patientReference, personIdCell);

        // set category to 'complaint'
        conditionBuilder.setAsProblem(true);
        //NOTE: the text of "complaint" is wrong in that it's not the same as a "problem", but this is the String that was used
        conditionBuilder.setCategory("complaint");

        // save resource
        if (LOG.isTraceEnabled()) {
            LOG.trace("Save Condition:" + FhirSerializationHelper.serializeResource(conditionBuilder.getResource()));
        }
        savePatientResourceMapIds(fhirResourceFiler, parser.getCurrentState(), conditionBuilder);
    }

    /*
     *
     */
    /*public static void createCondition(ProblemTable parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       HomertonCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode) throws Exception {
        CodeableConcept cc = null;
        Date d = null;

        // Organisation - Since EpisodeOfCare record is not established no need for Organization either
        // PatientTable
        //ResourceId patientResourceId = resolvePatientResource(parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getLocalPatientId(), null, null, null, null, null, null, null);
        // EpisodeOfCare - ProblemTable record cannot be linked to an EpisodeOfCare
        // EncounterTable - ProblemTable record cannot be linked to an EncounterTable
        // this ProblemTable resource id
        //ResourceId problemResourceId = getProblemResourceId(parser.getLocalPatientId(), parser.getOnsetDateAsString(), parser.getProblemCode());

        //CodeableConcept problemCode = new CodeableConcept();
        //problemCode.addCoding().setCode(parser.getProblemCode()).setSystem(getCodeSystemName(HomertonCsvToFhirTransformer.CODE_SYSTEM_SNOMED)).setDisplay(parser.getProblem());

        //Identifiers
        //Identifier identifiers[] = {new Identifier().setSystem(HomertonCsvToFhirTransformer.CODE_SYSTEM_PROBLEM_ID).setValue(parser.getProblemId().toString())};

        //DateTimeType onsetDate = new DateTimeType(parser.getOnsetDate());

        Condition fhirCondition = new Condition();
        //createConditionResource(fhirCondition, problemResourceId, patientResourceId, null, parser.getUpdateDateTime(), problemCode, onsetDate, parser.getAnnotatedDisp(), identifiers);

        //ResourceId patientResourceId = resolvePatientResource(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, null, parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getCNN(), parser.getNHSNo(), name, fhirAddress, convertSusGenderToFHIR(parser.getGender()), parser.getDOB(), organisationResourceId, null, patientIdentifier, gpResourceId, gpPracticeResourceId, ethnicGroup);

        //ReferenceHelper.createReference(ResourceType.PatientTable, patientResourceId.getResourceId().toString()));

        fhirCondition.setId(parser.getProblemId());

        // set patient reference
        fhirCondition.setPatient(ReferenceHelper.createReference(ResourceType.PatientTable, parser.getCNN()));

        // set category to 'complaint'
        cc = new CodeableConcept();
        cc.addCoding().setSystem(HomertonCsvToFhirTransformer.CODE_SYSTEM_CONDITION_CATEGORY).setCode("complaint");
        fhirCondition.setCategory(cc);

        // save resource
        LOG.debug("Save Condition:" + FhirSerializationHelper.serializeResource(fhirCondition));
        savePatientResourceMapIds(fhirResourceFiler, parser.getCurrentState(), fhirCondition.getId(), fhirCondition);
    }*/

    /*
     *
     */
    public static void createConditionResource(Condition fhirCondition, ResourceId problemResourceId, ResourceId patientResourceId, ResourceId encounterResourceId, Date dateRecorded, CodeableConcept problemCode, DateTimeType onsetDate, String notes, Identifier identifiers[]) throws Exception {
        CodeableConcept cc = null;
        Date d = null;

        // Turn problem_id into Resource id
        fhirCondition.setId(problemResourceId.getResourceId().toString());

        if (identifiers != null) {
            for (int i = 0; i < identifiers.length; i++) {
                fhirCondition.addIdentifier(identifiers[i]);
            }
        }

        if (encounterResourceId != null) {
            fhirCondition.setEncounter(ReferenceHelper.createReference(ResourceType.Encounter, encounterResourceId.getResourceId().toString()));
        }

        // Date recorded
        fhirCondition.setDateRecorded(dateRecorded);

        // set code to coded problem - field 28
        fhirCondition.setCode(problemCode);

        // set onset to field  to field 10 + 11
        fhirCondition.setOnset(onsetDate);

        // set notes
        if (notes != null) {
            fhirCondition.setNotes(notes);
        }

    }

}
