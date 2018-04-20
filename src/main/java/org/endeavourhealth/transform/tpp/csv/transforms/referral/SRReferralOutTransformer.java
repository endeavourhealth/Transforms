package org.endeavourhealth.transform.tpp.csv.transforms.referral;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.schema.ReferralPriority;
import org.endeavourhealth.common.fhir.schema.ReferralType;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppCtv3Lookup;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.dal.publisherTransform.models.TppConfigListOption;
import org.endeavourhealth.core.database.dal.publisherTransform.models.TppMappingRef;
import org.endeavourhealth.core.terminology.SnomedCode;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ReferralRequestBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.ReferralRequestResourceCache;
import org.endeavourhealth.transform.tpp.csv.schema.referral.SRReferralOut;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRReferralOutTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRReferralOutTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRReferralOut.class);
        while (parser.nextRecord()) {

            try {
                createResource((SRReferralOut)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    private static void createResource(SRReferralOut parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell referralOutId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        if (patientId.isEmpty()) {
            TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                    parser.getRowIdentifier().getString(), parser.getFilePath());
            return;
        }

        if (patientId.isEmpty()) {

            if (!deleteData.getIntAsBoolean()) {
                TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                        parser.getRowIdentifier().getString(), parser.getFilePath());
                return;
            } else {

                // get previously filed resource for deletion
                org.hl7.fhir.instance.model.ReferralRequest referralRequest
                        = (org.hl7.fhir.instance.model.ReferralRequest) csvHelper.retrieveResource(referralOutId.getString(),
                        ResourceType.ReferralRequest,
                        fhirResourceFiler);

                if (referralRequest != null) {
                    ReferralRequestBuilder referralRequestBuilder = new ReferralRequestBuilder(referralRequest);
                    fhirResourceFiler.deletePatientResource(parser.getCurrentState(), referralRequestBuilder);
                    return;
                }
            }
        }

        ReferralRequestBuilder referralRequestBuilder
                = ReferralRequestResourceCache.getReferralBuilder(referralOutId, patientId, csvHelper, fhirResourceFiler);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        referralRequestBuilder.setPatient(patientReference, patientId);

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {
            referralRequestBuilder.setRecordedDate(dateRecored.getDate(), dateRecored);
        }

        CsvCell referralDate = parser.getDateEvent();
        if (!referralDate.isEmpty()) {
            DateTimeType dateTimeType = new DateTimeType(referralDate.getDate());
            referralRequestBuilder.setDate(dateTimeType, referralDate);
        }

        CsvCell recordedBy = parser.getIDProfileEnteredBy();
        if (!recordedBy.isEmpty()) {

            String staffMemberId = csvHelper.getInternalId (InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID,
                                                             recordedBy.getString());
            Reference staffReference = csvHelper.createPractitionerReference(staffMemberId);
            referralRequestBuilder.setRecordedBy(staffReference, recordedBy);
        }

        CsvCell requestedByStaff = parser.getIDDoneBy();
        CsvCell requestedByOrg = parser.getIDOrganisationDoneAt();
        if (!requestedByStaff.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReference(requestedByStaff);
            referralRequestBuilder.setRequester(practitionerReference, requestedByStaff);
        } else if (!requestedByOrg.isEmpty()) {
            Reference orgReference = csvHelper.createOrganisationReference(requestedByOrg);
            referralRequestBuilder.setRequester(orgReference, requestedByOrg);
        }

        CsvCell referralType = parser.getTypeOfReferral();
        if (!referralType.isEmpty() && referralType.getLong()>0) {

            TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(referralType.getLong());
            if(!tppMappingRef.getMappedTerm().isEmpty()) {
                ReferralType type = convertReferralType(tppMappingRef.getMappedTerm());
                if (type != null) {
                    referralRequestBuilder.setType(type, referralType);
                } else {
                    referralRequestBuilder.setTypeFreeText(tppMappingRef.getMappedTerm(), referralType);
                }
            }
        }

        CsvCell reason = parser.getReason();
        if (!reason.isEmpty() && reason.getLong()>0) {

            TppConfigListOption tppConfigListOption = csvHelper.lookUpTppConfigListOption(reason.getLong());
            if (tppConfigListOption != null) {
                String referralReason = tppConfigListOption.getListOptionName();
                if (!Strings.isNullOrEmpty(referralReason)) {
                    referralRequestBuilder.setReasonFreeText(referralReason, reason);
                }
            }
        }

        CsvCell serviceOffered = parser.getServiceOffered();
        if (!serviceOffered.isEmpty() && serviceOffered.getLong()>0) {

            TppConfigListOption tppConfigListOption = csvHelper.lookUpTppConfigListOption(serviceOffered.getLong());
            if (tppConfigListOption != null) {
                String referralServiceOffered = tppConfigListOption.getListOptionName();
                if (!Strings.isNullOrEmpty(referralServiceOffered)) {
                    referralRequestBuilder.setServiceRequestedFreeText(referralServiceOffered, serviceOffered);
                }
            }
        }

        CsvCell referralPriority = parser.getUrgency();
        if (!referralPriority.isEmpty()) {

            TppConfigListOption tppConfigListOption = csvHelper.lookUpTppConfigListOption(referralType.getLong());
            if (tppConfigListOption != null) {
                ReferralPriority priority = convertPriority(tppConfigListOption.getListOptionName());
                if (priority != null) {
                    referralRequestBuilder.setPriority(priority, referralPriority);
                } else {
                    referralRequestBuilder.setPriorityFreeText(referralPriority.getString(), referralPriority);
                }
            }
        }

        //code is Ctv3 so translate to Snomed
        CsvCell referralPrimaryDiagnosisCode = parser.getPrimaryDiagnosis();
        if (!referralPrimaryDiagnosisCode.isEmpty()) {

            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(referralRequestBuilder, ReferralRequestBuilder.TAG_REASON_CODEABLE_CONCEPT);

            // add Ctv3 coding
            TppCtv3Lookup ctv3Lookup = csvHelper.lookUpTppCtv3Code(referralPrimaryDiagnosisCode.getString());

            if (ctv3Lookup != null) {
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CTV3);
                codeableConceptBuilder.setCodingCode(referralPrimaryDiagnosisCode.getString(), referralPrimaryDiagnosisCode);
                String readV3Term = ctv3Lookup.getCtv3Text();
                //TODO - need to carry through the audit of where this term came from, from the audit info on TppCtv3Lookup
                codeableConceptBuilder.setCodingDisplay(readV3Term, null);
                codeableConceptBuilder.setText(readV3Term, null);
            }

            // Only try to transform to snomed if the code doesn't start with "Y" (local codes start with "Y")
            if (!referralPrimaryDiagnosisCode.getString().startsWith("Y")) {
                // translate to Snomed
                SnomedCode snomedCode = TerminologyService.translateCtv3ToSnomed(referralPrimaryDiagnosisCode.getString());
                if (snomedCode != null) {

                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
                    codeableConceptBuilder.setCodingCode(snomedCode.getConceptCode());
                    codeableConceptBuilder.setCodingDisplay(snomedCode.getTerm());
                    codeableConceptBuilder.setText(snomedCode.getTerm());
                }
            }
        }

        CsvCell referralRecipientType = parser.getRecipientIDType();
        if (!referralRecipientType.isEmpty()) {

            CsvCell referralRecipientId = parser.getRecipientID();
            if (recipientIsPerson(referralRecipientType.getLong(), csvHelper)) {
                Reference practitionerReference = csvHelper.createPractitionerReference(referralRecipientId);
                referralRequestBuilder.addRecipient(practitionerReference, referralRecipientId);
            } else {
                Reference orgReference = csvHelper.createOrganisationReference(referralRecipientId);
                referralRequestBuilder.addRecipient(orgReference, referralRecipientId);
            }
        }

        // set consultation/encounter reference
        CsvCell eventId = parser.getIDEvent();
        if (!eventId.isEmpty()) {

            Reference eventReference = csvHelper.createEncounterReference(eventId, patientId);
            referralRequestBuilder.setEncounter (eventReference, eventId);
        }
    }

    private static ReferralPriority convertPriority(String priority) {

        if (priority.equalsIgnoreCase("routine")) {
            return ReferralPriority.ROUTINE;

        } else if (priority.equalsIgnoreCase("urgent")) {
            return ReferralPriority.URGENT;

        } else if (priority.equalsIgnoreCase("2 week wait")) {
            return ReferralPriority.TWO_WEEK_WAIT;

        } else if (priority.equalsIgnoreCase("soon")) {
            return ReferralPriority.SOON;

        } else {
            return null;
        }
    }

    private static Boolean recipientIsPerson (Long recipientTypeId, TppCsvHelper csvHelper) throws Exception {

        TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(recipientTypeId);
        String term = tppMappingRef.getMappedTerm();

        if (term.toLowerCase().startsWith("organisation")) {
            return false;
        }

        return true;
    }

    private static ReferralType convertReferralType(String type) {

        if (type.equalsIgnoreCase("Unknown")) {
            return ReferralType.UNKNOWN;

        } else if (type.equalsIgnoreCase("Assessment")) {
            return ReferralType.ASSESSMENT;

        } else if (type.equalsIgnoreCase("Investigation")) {
            return ReferralType.INVESTIGATION;

        } else if (type.equalsIgnoreCase("Management advice")) {
            return ReferralType.MANAGEMENT_ADVICE;

        } else if (type.equalsIgnoreCase("Patient reassurance")) {
            return ReferralType.PATIENT_REASSURANCE;

        } else if (type.equalsIgnoreCase("Self referral")) {
            return ReferralType.SELF_REFERRAL;

        } else if (type.equalsIgnoreCase("Treatment")) {
            return ReferralType.TREATMENT;

        } else if (type.equalsIgnoreCase("Outpatient")) {
            return ReferralType.OUTPATIENT;

        } else if (type.equalsIgnoreCase("Community Care")) {
            return ReferralType.COMMUNITY_CARE;

        } else if (type.equalsIgnoreCase("Performance of a procedure / operation")) {
            return ReferralType.PROCEDURE;

        } else if (type.equalsIgnoreCase("Admission")) {
            return ReferralType.ADMISSION;

        } else if (type.equalsIgnoreCase("Day Care")) {
            return ReferralType.DAY_CARE;

        } else if (type.equalsIgnoreCase("Assessment & Education")) {
            return ReferralType.ASSESSMENT_AND_EDUCATION;

        } else {
            return null;
        }
    }

}
