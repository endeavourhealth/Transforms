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
import org.endeavourhealth.transform.homerton.schema.DiagnosisTable;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public class DiagnosisTransformer extends HomertonBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(DiagnosisTransformer.class);

    public static void transform(String version,
                                 List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonCsvHelper csvHelper,
                                 String primaryOrgOdsCode) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    String valStr = validateEntry((DiagnosisTable) parser);
                    if (valStr == null) {
                        createDiagnosis((DiagnosisTable) parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode);
                    } else {
                        TransformWarnings.log(LOG, parser, "Validation error: {}", valStr);
                    }
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    /*
     *
     */
    public static String validateEntry(DiagnosisTable parser) {
        return null;
    }

    /*
     *
     */
    public static void createDiagnosis(DiagnosisTable parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       HomertonCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode) throws Exception {

        CsvCell diagnosisId = parser.getDiagnosisId();
        CsvCell personIdCell = parser.getPersonId();

        //TODO - fix to not use HL7 DB as a mapping store
        if (true) {
            throw new RuntimeException("FIX CODE");
        }
        ResourceId conditionResourceId = null;
        //ResourceId conditionResourceId = getOrCreateConditionResourceId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, diagnosisId);

        // PatientTable
        UUID patientUuid = csvHelper.findPatientIdFromPersonId(personIdCell);
        if (patientUuid == null) {
            TransformWarnings.log(LOG, parser, "Skipping DiagnosisTable {} because no Person->MRN mapping ({}) could be found in file {}", diagnosisId.getString(), personIdCell.getString(), parser.getFilePath());
            return;
        }

        // create the FHIR Condition
        ConditionBuilder conditionBuilder = new ConditionBuilder();
        conditionBuilder.setAsProblem(false);

        conditionBuilder.setId(conditionResourceId.getResourceId().toString());

        Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString());
        conditionBuilder.setPatient(patientReference, personIdCell);

        // Organisation - Since EpisodeOfCare record is not established no need for Organization either

        // EpisodeOfCare - DiagnosisTable record cannot be linked to an EpisodeOfCare

        // fhirCondition.setEncounter()

        // this DiagnosisTable resource id
        //ResourceId diagnosisResourceId = getDiagnosisResourceId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, parser.getCNN(), parser.getDiagnosisDateAsString(), parser.getDiagnosisCode());

        //CodeableConcept diagnosisCode = new CodeableConcept();
        //diagnosisCode.addCoding().setSystem(getCodeSystemName(HomertonCsvToFhirTransformer.CODE_SYSTEM_SNOMED)).setDisplay(parser.getDiagnosis()).setCode(parser.getDiagnosisCode());
        //CodeableConcept diagnosisCode = CodeableConceptHelper.createCodeableConcept(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, parser.getDiagnosis(), parser.getDiagnosisCode());

        //Identifiers
        //Identifier identifiers[] = {new Identifier().setSystem(HomertonCsvToFhirTransformer.CODE_SYSTEM_DIAGNOSIS_ID).setValue(parser.getDiagnosisId().toString()), new Identifier().setSystem(HomertonCsvToFhirTransformer.CODE_SYSTEM_FIN_NO).setValue(parser.getFINNbr())};

        //createDiagnosisResource(fhirCondition, diagnosisResourceId, encounterResourceId, patientResourceId, parser.getUpdateDateTime(), new DateTimeType(parser.getDiagnosisDate()), diagnosisCode, parser.getSecondaryDescription(), identifiers, cvs);

        // save resource
        LOG.debug("Save Diagnosis (PatId=" + patientUuid + ")(PersonId:" + personIdCell.getString() + "):" + FhirSerializationHelper.serializeResource(conditionBuilder.getResource()));
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), conditionBuilder);

    }


}
