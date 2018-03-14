package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.AddressConverter;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.Problem;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class ProblemTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ProblemTransformer.class);

    /*
     *
     */
    public static void transform(String version,
                                 List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        for (ParserI parser: parsers) {

            while (parser.nextRecord()) {
                try {
                    String valStr = validateEntry((Problem)parser);
                    if (valStr == null) {
                        createConditionProblem((Problem)parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                    } else {
                        LOG.debug("Validation error:" + valStr);
                        SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, valStr);
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
    public static String validateEntry(Problem parser) {
        CsvCell cell = parser.getLocalPatientId();
        if (cell.isEmpty()) {
            return "LocalPatientId not found for problemId " + parser.getProblemId();
        } else {
            return null;
        }
    }

    /*
    *
    */
    public static void createConditionProblem(Problem parser,
                                              FhirResourceFiler fhirResourceFiler,
                                              BartsCsvHelper csvHelper,
                                              String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {
        // Organisation
        Address fhirOrgAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
        ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);

        // Patient
        CsvCell patientIdCell = parser.getLocalPatientId();
        Identifier patientIdentifier[] = {new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID).setValue(patientIdCell.getString())};
        ResourceId patientResourceId = resolvePatientResource(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, null, parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, patientIdCell.getString(), null, null, null, null, null, organisationResourceId, null, patientIdentifier, null, null, null);
        // EpisodeOfCare - Problem record cannot be linked to an EpisodeOfCare
        // Encounter - Problem record cannot be linked to an Encounter
        // this Problem resource id

        CsvCell onsetDateCell = parser.getOnsetDate();
        CsvCell problemCodeCell = parser.getProblemCode();
        ResourceId problemResourceId = getProblemResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, patientIdCell.getString(), onsetDateCell.getString(), problemCodeCell.getString());

        ConditionBuilder conditionBuilder = new ConditionBuilder();
        //createProblemResource(fhirCondition, problemResourceId, patientResourceId, null, parser.getUpdateDateTime(), problemCode, onsetDate, parser.getAnnotatedDisp(), identifiers, ex, cvs);
        //****************************************************************
        conditionBuilder.setId(problemResourceId.getResourceId().toString());

        conditionBuilder.setAsProblem(true);

        //Identifiers
        IdentifierBuilder ib = new IdentifierBuilder(conditionBuilder);
        ib.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_PROBLEM_ID);
        ib.setValue(parser.getProblemId().toString());

        // Extensions
        conditionBuilder.setContext("clinical coding");

        // set patient reference
        conditionBuilder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));

        // Date recorded
        CsvCell updatedDateCell = parser.getUpdateDateTime();
        Date updatedDate = updatedDateCell.getDate();
        conditionBuilder.setRecordedDate(updatedDate, updatedDateCell);

        // set code to coded problem
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(conditionBuilder, ConditionBuilder.TAG_CODEABLE_CONCEPT_CODE);

        //set the raw term on the codeable concept text
        CsvCell problemTermCell = parser.getProblem();
        String suppliedTerm = problemTermCell.getString();
        codeableConceptBuilder.setText(suppliedTerm, problemTermCell);

        //it's rare, but there are cases where records have a textual term but not vocab or code
        CsvCell vocabCell = parser.getVocabulary();
        if (!vocabCell.isEmpty() && !problemCodeCell.isEmpty()) {
            String vocab = vocabCell.getString();
            String code = problemCodeCell.getString();

            if (vocab.equalsIgnoreCase("SNOMED CT")) {
                String term = TerminologyService.lookupSnomedTerm(code);
                if (Strings.isNullOrEmpty(term)) {
                    TransformWarnings.log(LOG, parser, "Failed to lookup Snomed term for code {}", code);
                }

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, vocabCell);
                codeableConceptBuilder.setCodingCode(code, problemCodeCell);
                codeableConceptBuilder.setCodingDisplay(term); //don't pass in the cell as this is derived

            } else if (vocab.equalsIgnoreCase("ICD-10")) {
                String term = TerminologyService.lookupIcd10CodeDescription(code);
                if (Strings.isNullOrEmpty(term)) {
                    TransformWarnings.log(LOG, parser, "Failed to lookup ICD-10 term for code {}", code);
                }

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10, vocabCell);
                codeableConceptBuilder.setCodingCode(code, problemCodeCell);
                codeableConceptBuilder.setCodingDisplay(term); //don't pass in the cell as this is derived

            } else if (vocab.equalsIgnoreCase("Cerner")) {
                //in this file, Cerner VOCAB doesn't seem to mean it refers to the CVREF file, so don't make any attempt to look up an official term
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID, vocabCell);
                codeableConceptBuilder.setCodingCode(code, problemCodeCell);

            } else {
                TransformWarnings.log(LOG, parser, "Problem {} has unknown VOCAB value [{}] in file {}", parser.getProblemId(), vocab, parser.getFilePath());

                codeableConceptBuilder.addCoding(null, vocabCell);
                codeableConceptBuilder.setCodingCode(code, problemCodeCell);
            }
        }

        // set category to 'complaint'
        conditionBuilder.setCategory("complaint");

        // set onset to field  to field 10 + 11
        DateTimeType onsetDate = new DateTimeType(onsetDateCell.getDate());
        conditionBuilder.setOnset(onsetDate, onsetDateCell);

        CsvCell statusCell = parser.getStatusLifecycle();
        String status = statusCell.getString();
        if (status.equalsIgnoreCase("Canceled")) { //note the US spelling used

            //LOG.debug("Delete Condition(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
            //deletePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), conditionBuilder);
            return;

        } else if (status.equalsIgnoreCase("Confirmed")) {
            conditionBuilder.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED, statusCell);

        } else {
            conditionBuilder.setVerificationStatus(Condition.ConditionVerificationStatus.PROVISIONAL, statusCell);
        }

        // set notes
        CsvCell annotatedDisplayCell = parser.getAnnotatedDisp();
        if (!annotatedDisplayCell.isEmpty()) {
            conditionBuilder.setNotes(annotatedDisplayCell.getString(), annotatedDisplayCell);
        }
        //****************************************************************

        // save resource
        //LOG.debug("Save Condition(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
        //savePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), conditionBuilder);
    }

}
