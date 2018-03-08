package org.endeavourhealth.transform.barts.transforms;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.BulkProblem;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class BulkProblemTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(BulkProblemTransformer.class);

    /*
     *
     */
    public static void transform(String version,
                                 ParserI parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        if (parser == null) {
            return;
        }

        while (parser.nextRecord()) {
            try {
                createConditionProblem((BulkProblem)parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);

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
                                              BartsCsvHelper csvHelper,
                                              String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {
        CodeableConcept cc = null;
        Date d = null;

        // Organisation
        Address fhirOrgAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
        ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);

        // Patient
        CsvCell localPatientIdCell = parser.getLocalPatientId();
        String localPatientId = localPatientIdCell.getString();
        Identifier patientIdentifier[] = {new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID).setValue(StringUtils.deleteWhitespace(localPatientId))};
        ResourceId patientResourceId = getPatientResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, primaryOrgHL7OrgOID, localPatientId);
        if (patientResourceId == null) {
            patientResourceId = resolvePatientResource(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, null, parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, localPatientId, null, null, null, null, null, organisationResourceId, null, patientIdentifier, null, null, null);
        }
        // EpisodeOfCare - Problem record cannot be linked to an EpisodeOfCare

        // Encounter - Problem record cannot be linked to an Encounter

        // this Problem resource id
        CsvCell onsetDateCell = parser.getOnsetDate();
        CsvCell problemCodeCell = parser.getProblemCode();
        CsvCell problemTermCell = parser.getProblem();

        CsvCell problemIdCell = parser.getProblemId();
        String problemId = problemIdCell.getString().split("\\.")[0];

        ResourceId problemResourceId = readProblemResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, localPatientId, onsetDateCell.getString(), problemCodeCell.getString());
        if (problemResourceId == null) {
            problemResourceId = getProblemResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, localPatientId, onsetDateCell.getString(), problemCodeCell.getString());

            CodeableConcept problemCode = CodeableConceptHelper.createCodeableConcept(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, problemTermCell.getString(), problemCodeCell.getString());

            //Identifiers
            Identifier identifiers[] = {new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_PROBLEM_ID).setValue(problemId)};

            DateTimeType onsetDate = new DateTimeType(onsetDateCell.getDate());

            Extension[] ex = {ExtensionConverter.createStringExtension(FhirExtensionUri.RESOURCE_CONTEXT, "clinical coding")};

            CsvCell statusCell = parser.getStatusLifecycle();
            String status = statusCell.getString();

            Condition.ConditionVerificationStatus cvs;
            if (status.equalsIgnoreCase("Canceled")) {
                cvs = Condition.ConditionVerificationStatus.ENTEREDINERROR;
            } else if (status.equalsIgnoreCase("Confirmed")) {
                cvs = Condition.ConditionVerificationStatus.CONFIRMED;
            } else {
                cvs = Condition.ConditionVerificationStatus.PROVISIONAL;
            }

            Date updateDate = parser.getUpdateDateTime().getDate();
            String comments = parser.getAnnotatedDisp().getString();

            Condition fhirCondition = new Condition();
            createProblemResource(fhirCondition, problemResourceId, patientResourceId, null, updateDate, problemCode, onsetDate, comments, identifiers, ex, cvs);

            // save resource
            LOG.debug("Save Condition(PatId=" + localPatientId + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
            savePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
        }

    }

}
