package org.endeavourhealth.transform.barts.transforms;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.BulkProblem;
import org.endeavourhealth.transform.barts.schema.Problem;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.hl7.fhir.instance.model.CodeableConcept;
import org.hl7.fhir.instance.model.Condition;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Extension;
import org.hl7.fhir.instance.model.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class BulkProblemTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(BulkProblemTransformer.class);

    /*
     *
     */
    public static void transform(String version,
                                 BulkProblem parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        // Skip header line
        parser.nextRecord();

        while (parser.nextRecord()) {
            try {
                createConditionProblem(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);

            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

    }

    /*
*
*/
    public static void createConditionProblem(BulkProblem parser,
                                              FhirResourceFiler fhirResourceFiler,
                                              EmisCsvHelper csvHelper,
                                              String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {
        CodeableConcept cc = null;
        Date d = null;

        // Organisation - Since EpisodeOfCare record is not established no need for Organization either

        // Patient
        Identifier patientIdentifier[] = {new Identifier().setSystem(FhirUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID).setValue(StringUtils.deleteWhitespace(parser.getLocalPatientId()))};
        ResourceId patientResourceId = getPatientResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, primaryOrgHL7OrgOID, parser.getLocalPatientId());
        if (patientResourceId == null) {
            patientResourceId = resolvePatientResource(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, null, parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getLocalPatientId(), null, null, null, null, null, null, null, patientIdentifier, null, null, null);
        }
        // EpisodeOfCare - Problem record cannot be linked to an EpisodeOfCare

        // Encounter - Problem record cannot be linked to an Encounter

        // this Problem resource id
        ResourceId problemResourceId = readProblemResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getLocalPatientId(), parser.getOnsetDateAsString(), parser.getProblemCode());
        if (problemResourceId == null) {
            problemResourceId = getProblemResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getLocalPatientId(), parser.getOnsetDateAsString(), parser.getProblemCode());

            CodeableConcept problemCode = CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_SNOMED_CT, parser.getProblem(), parser.getProblemCode());

            //Identifiers
            Identifier identifiers[] = {new Identifier().setSystem(BartsCsvToFhirTransformer.CODE_SYSTEM_PROBLEM_ID).setValue(parser.getProblemId().toString())};

            DateTimeType onsetDate = new DateTimeType(parser.getOnsetDate());

            Extension[] ex = {ExtensionConverter.createStringExtension(FhirExtensionUri.RESOURCE_CONTEXT, "clinical coding")};

            Condition.ConditionVerificationStatus cvs;
            if (parser.getStatusLifecycle().compareToIgnoreCase("Canceled") == 0) {
                cvs = Condition.ConditionVerificationStatus.ENTEREDINERROR;
            } else {
                if (parser.getConfirmation().compareToIgnoreCase("Confirmed") == 0) {
                    cvs = Condition.ConditionVerificationStatus.CONFIRMED;
                } else {
                    cvs = Condition.ConditionVerificationStatus.PROVISIONAL;
                }
            }

            Condition fhirCondition = new Condition();
            createProblemResource(fhirCondition, problemResourceId, patientResourceId, null, parser.getUpdateDateTime(), problemCode, onsetDate, parser.getAnnotatedDisp(), identifiers, ex, cvs);

            // save resource
            LOG.debug("Save Condition(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
            savePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
        }

    }

}
