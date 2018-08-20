package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.schema.ReferralPriority;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppCtv3Lookup;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMappingRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.TppConfigListOption;
import org.endeavourhealth.core.terminology.SnomedCode;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ReferralRequestBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.referral.SRReferralOut;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SRReferralOutTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRReferralOutTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRReferralOut.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRReferralOut) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createResource(SRReferralOut parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell referralOutId = parser.getRowIdentifier();

        ReferralRequestBuilder referralRequestBuilder = csvHelper.getReferralRequestResourceCache().getReferralBuilder(referralOutId, csvHelper);

        CsvCell deleteData = parser.getRemovedData();
        if (deleteData != null && !deleteData.getIntAsBoolean()) {
            csvHelper.getReferralRequestResourceCache().addToDeletes(referralOutId, referralRequestBuilder);
            return;
        }

        CsvCell patientId = parser.getIDPatient();
        Reference patientReference = csvHelper.createPatientReference(patientId);
        if (referralRequestBuilder.isIdMapped()) {
            patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
        }
        referralRequestBuilder.setPatient(patientReference, patientId);

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {
            referralRequestBuilder.setRecordedDate(dateRecored.getDateTime(), dateRecored);
        }

        CsvCell referralDate = parser.getDateEvent();
        if (!referralDate.isEmpty()) {
            DateTimeType dateTimeType = new DateTimeType(referralDate.getDateTime());
            referralRequestBuilder.setDate(dateTimeType, referralDate);
        }

        CsvCell profileIdRecordedBy = parser.getIDProfileEnteredBy();
        if (!profileIdRecordedBy.isEmpty()) {
            Reference staffReference = csvHelper.createPractitionerReferenceForProfileId(profileIdRecordedBy);
            if (referralRequestBuilder.isIdMapped()) {
                staffReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(staffReference, fhirResourceFiler);
            }
            referralRequestBuilder.setRecordedBy(staffReference, profileIdRecordedBy);
        }

        CsvCell staffMemberIdDoneBy = parser.getIDDoneBy();
        CsvCell requestedByOrg = parser.getIDOrganisationDoneAt();
        if (!staffMemberIdDoneBy.isEmpty() && staffMemberIdDoneBy.getLong() > -1) {
            Reference practitionerReference = csvHelper.createPractitionerReferenceForStaffMemberId(staffMemberIdDoneBy, parser.getIDProfileEnteredBy(), parser.getIDOrganisationDoneAt());
            if (referralRequestBuilder.isIdMapped()) {
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
            }
            referralRequestBuilder.setRequester(practitionerReference, staffMemberIdDoneBy);

        } else if (!requestedByOrg.isEmpty()) {
            Reference orgReference = csvHelper.createOrganisationReference(requestedByOrg);
            if (referralRequestBuilder.isIdMapped()) {
                orgReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(orgReference, fhirResourceFiler);
            }
            referralRequestBuilder.setRequester(orgReference, requestedByOrg);
        }

        //NOTE: the TPP TypeOfReferral and ServiceOffered essentially represent the same concept -
        //a high-level descriptor of the type of the service being referred to (e.g. Hospital or Physiotherapy).
        //Because they're VERY rarely used in SystmOne and represent similar concepts, both values, if present,
        //will be stored in a new extension
        List<String> recipientServiceTypes = new ArrayList<>();

        CsvCell serviceOffered = parser.getServiceOffered();
        if (!serviceOffered.isEmpty()) {
            //the documentation states that service offered is a configured list option, but that's
            //incorrect. In fact, this field refers to the SRMapping file (stored in the tpp_mapping_ref table).

            //the vast majority of rows seem to refer to row ID which doesn't exist and is
            //weirdly enough the group ID of all the configured list items (i.e. org type)
            //so only look up if it's not this value, since we believe this value means that the field isn't set
            if (serviceOffered.getLong().longValue() != 175137) {

                TppMappingRef mapping = csvHelper.lookUpTppMappingRef(serviceOffered);
                if (mapping != null) {
                    String term = mapping.getMappedTerm();
                    recipientServiceTypes.add(term);
                }
            }
        }

        CsvCell referralType = parser.getTypeOfReferral();
        if (!referralType.isEmpty()) {
            //TPP type of referral is a high-level of the the service type being referred so
            TppMappingRef mapping = csvHelper.lookUpTppMappingRef(referralType);
            if (mapping != null) {
                String term = mapping.getMappedTerm();
                recipientServiceTypes.add(term);
            }
        }

        //note do this even if empty, since we may be updating an existing referral resource and want to clear the field
        String recipientServiceType = String.join(", ", recipientServiceTypes);
        referralRequestBuilder.setRecipientServiceType(recipientServiceType, serviceOffered);

        CsvCell reason = parser.getReason();
        if (!reason.isEmpty() && reason.getInt() != -1) {
            //the TPP referral reason is really the objective of the referral
            //e.g. Advice/Consultation or Advice and Support
            TppConfigListOption tppConfigListOption = csvHelper.lookUpTppConfigListOption(reason, parser);
            if (tppConfigListOption != null) {
                //not going to attempt to map the term to the ReferralType values we have,
                //since there are 1000+ possible options in TPP, so we really need the Information Model to support this properly
                String term = tppConfigListOption.getListOptionName();
                referralRequestBuilder.setTypeFreeText(term, reason);
            }
        }

        CsvCell referralPriority = parser.getUrgency();
        if (!referralPriority.isEmpty() && referralPriority.getInt() != -1) {

            TppConfigListOption tppConfigListOption = csvHelper.lookUpTppConfigListOption(referralPriority, parser);
            if (tppConfigListOption != null) {
                ReferralPriority priority = convertPriority(tppConfigListOption.getListOptionName());
                if (priority != null) {
                    referralRequestBuilder.setPriority(priority, referralPriority);

                } else {
                    referralRequestBuilder.setPriorityFreeText(referralPriority.getString(), referralPriority);
                    TransformWarnings.log(LOG, csvHelper, "Unmapped TPP referral priority {}. Setting free text.", referralPriority.getString());
                }
            }
        }

        //code is Ctv3 so translate to Snomed
        CsvCell referralPrimaryDiagnosisCode = parser.getPrimaryDiagnosis();
        if (!referralPrimaryDiagnosisCode.isEmpty()) {

            //we may have retrieved the Resource from the DB, so clear out any existing codeable concept
            if (referralRequestBuilder.hasCodeableConcept(CodeableConceptBuilder.Tag.Referral_Request_Service)) {
                referralRequestBuilder.removeCodeableConcept(CodeableConceptBuilder.Tag.Referral_Request_Service, null);
            }
            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(referralRequestBuilder, CodeableConceptBuilder.Tag.Referral_Request_Service);

            // add Ctv3 coding
            TppCtv3Lookup ctv3Lookup = csvHelper.lookUpTppCtv3Code(referralPrimaryDiagnosisCode.getString(), parser);

            if (ctv3Lookup != null) {
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CTV3);
                codeableConceptBuilder.setCodingCode(referralPrimaryDiagnosisCode.getString(), referralPrimaryDiagnosisCode);
                String readV3Term = ctv3Lookup.getCtv3Text();
                //TODO - need to carry through the audit of where this term came from, from the audit info on TppCtv3Lookup
                if (Strings.isNullOrEmpty(readV3Term)) {
                    codeableConceptBuilder.setCodingDisplay(readV3Term, referralPrimaryDiagnosisCode);
                    codeableConceptBuilder.setText(readV3Term, referralPrimaryDiagnosisCode);
                }
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
            if (!referralRecipientId.isEmpty()) {
                //TODO - restore this after understanding what the field actually contains
                /*if (recipientIsPerson(referralRecipientType, csvHelper, parser)) {
                    Reference practitionerReference = csvHelper.createPractitionerReferenceForProfileId(referralRecipientId);
                    if (referralRequestBuilder.isIdMapped()) {
                        practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                    }
                    referralRequestBuilder.addRecipient(practitionerReference, referralRecipientId);

                } else {
                    Reference orgReference = csvHelper.createOrganisationReference(referralRecipientId);
                    if (referralRequestBuilder.isIdMapped()) {
                        orgReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(orgReference, fhirResourceFiler);
                    }
                    referralRequestBuilder.addRecipient(orgReference, referralRecipientId);
                }*/
            }
        }

        // set consultation/encounter reference
        CsvCell eventId = parser.getIDEvent();
        if (!eventId.isEmpty()) {

            Reference eventReference = csvHelper.createEncounterReference(eventId);
            if (referralRequestBuilder.isIdMapped()) {
                eventReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(eventReference, fhirResourceFiler);
            }
            referralRequestBuilder.setEncounter(eventReference, eventId);
        }
//        boolean mapIds = !referralRequestBuilder.isIdMapped();
//        ResourceValidatorReferralRequest resourceValidatorReferralRequest = new ResourceValidatorReferralRequest();
//        List<String> problems = new ArrayList<String>();
//        try {
//            resourceValidatorReferralRequest.validateResourceSave(referralRequestBuilder.getResource(),
//                    fhirResourceFiler.getServiceId(), mapIds, problems);
//        } catch (Exception TransformException) {
//            LOG.info("Validation exception caught");
//            // NOP
//        }
//        if (problems.isEmpty()) {
//            //LOG.info("Validator passed");
//            fhirResourceFiler.savePatientResource(parser.getCurrentState(), mapIds, referralRequestBuilder);
//        } else {
//            //LOG.info("Validator failed");
//            for (String s: problems) {
//                LOG.info("ResourceSaveValidator problem:" + s);
//            }
//            // filed in cache
//            // fhirResourceFiler.savePatientResource(parser.getCurrentState(), !mapIds, referralRequestBuilder);
//        }
    }

    private static ReferralPriority convertPriority(String priority) {

        if (priority.equalsIgnoreCase("routine")) {
            return ReferralPriority.ROUTINE;

        } else if (priority.equalsIgnoreCase("urgent")) {
            return ReferralPriority.URGENT;

        } else if (priority.equalsIgnoreCase("2 week wait")
                || priority.equalsIgnoreCase("Two-Week Wait")
                || priority.trim().equals("8865")) {
            return ReferralPriority.TWO_WEEK_WAIT;

        } else if (priority.equalsIgnoreCase("soon")) {
            return ReferralPriority.SOON;

        } else {
            LOG.info("Unknown:" + priority + "<>not mapped.");
            return null;
        }
    }

    public static Boolean recipientIsPerson(CsvCell recipientTypeCell, TppCsvHelper csvHelper, AbstractCsvParser parser) throws Exception {

        TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(recipientTypeCell);
        if (tppMappingRef == null) {
            return false;
        }

        String term = tppMappingRef.getMappedTerm();
        if (term.equals("Organisation ID")) {
            return false;
        }

        //TODO - properly handle the five different types of data
        //known options are:
        /*
        "Unknown"
        "GMC Number"
        "NMC Number"
        "Organisation ID"
        "Other ID"
        */

        return true;
    }


}
