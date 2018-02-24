package org.endeavourhealth.transform.homerton.transforms;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.homerton.schema.Diagnosis;
import org.hl7.fhir.instance.model.Condition;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiagnosisTransformer extends HomertonBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(DiagnosisTransformer.class);

    public static void transform(String version,
                                 Diagnosis parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper,
                                 String primaryOrgOdsCode) throws Exception {

        // Skip header line
        parser.nextRecord();

        while (parser.nextRecord()) {
            try {
                createDiagnosis(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode);

            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

    }

    /*
     *
     */
    public static void createDiagnosis(Diagnosis parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode) throws Exception {

        Condition fhirCondition = new Condition();
        fhirCondition.setId(String.valueOf(parser.getDiagnosisId().longValue()));

        fhirCondition.setPatient(ReferenceHelper.createReference(ResourceType.Patient, parser.getCNN()));

        // Organisation - Since EpisodeOfCare record is not established no need for Organization either

        // EpisodeOfCare - Diagnosis record cannot be linked to an EpisodeOfCare

        // fhirCondition.setEncounter()

        // this Diagnosis resource id
        //ResourceId diagnosisResourceId = getDiagnosisResourceId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, parser.getCNN(), parser.getDiagnosisDateAsString(), parser.getDiagnosisCode());

        Condition.ConditionVerificationStatus cvs;
        if (parser.getActiveIndicator()) {
            cvs = Condition.ConditionVerificationStatus.CONFIRMED;
        } else {
            cvs = Condition.ConditionVerificationStatus.ENTEREDINERROR;
        }

        //CodeableConcept diagnosisCode = new CodeableConcept();
        //diagnosisCode.addCoding().setSystem(getCodeSystemName(HomertonCsvToFhirTransformer.CODE_SYSTEM_SNOMED)).setDisplay(parser.getDiagnosis()).setCode(parser.getDiagnosisCode());
        //CodeableConcept diagnosisCode = CodeableConceptHelper.createCodeableConcept(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, parser.getDiagnosis(), parser.getDiagnosisCode());

        //Identifiers
        //Identifier identifiers[] = {new Identifier().setSystem(HomertonCsvToFhirTransformer.CODE_SYSTEM_DIAGNOSIS_ID).setValue(parser.getDiagnosisId().toString()), new Identifier().setSystem(HomertonCsvToFhirTransformer.CODE_SYSTEM_FIN_NO).setValue(parser.getFINNbr())};

        //createDiagnosisResource(fhirCondition, diagnosisResourceId, encounterResourceId, patientResourceId, parser.getUpdateDateTime(), new DateTimeType(parser.getDiagnosisDate()), diagnosisCode, parser.getSecondaryDescription(), identifiers, cvs);

        // save resource
        LOG.debug("Save Condition(PatId=" + parser.getCNN() + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
        savePatientResourceMapIds(fhirResourceFiler, parser.getCurrentState(), fhirCondition.getId(), fhirCondition);
    }


}
