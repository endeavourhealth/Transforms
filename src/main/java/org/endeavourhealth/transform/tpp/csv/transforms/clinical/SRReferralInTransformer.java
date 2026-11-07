package org.endeavourhealth.transform.tpp.csv.transforms.clinical;


import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.schema.ReferralPriority;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppConfigListOption;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMappingRef;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ReferralRequestBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCodingHelper;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.helpers.cache.ReferralStatusRecord;
import org.endeavourhealth.transform.tpp.csv.helpers.cache.TppReferralStatusCache;
import org.endeavourhealth.transform.tpp.csv.schema.referral.SRReferralIn;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ReferralRequest;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SRReferralInTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRReferralInTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRReferralIn.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRReferralIn) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createResource(SRReferralIn parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell referralIdCell = parser.getRowIdentifier();
        CsvCell removeDataCell = parser.getRemovedData();


        ReferralRequestBuilder referralRequestBuilder = null;
        ReferralRequest referralRequest = (ReferralRequest) csvHelper.retrieveResource(referralIdCell.getString(), ResourceType.ReferralRequest);
        if (referralRequest == null) {
            referralRequestBuilder = new ReferralRequestBuilder();
            referralRequestBuilder.setId(referralIdCell.getString(), referralIdCell);

        } else {
            referralRequestBuilder = new ReferralRequestBuilder(referralRequest);
        }


        if (removeDataCell != null && removeDataCell.getIntAsBoolean()) {
            if (referralRequestBuilder.isIdMapped()) { //only delete if ID mapped (i.e. previously saved)
                referralRequestBuilder.setDeletedAudit(removeDataCell);
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, referralRequestBuilder);
            }
            return;
        }

        CsvCell patientId = parser.getIDPatient();
        Reference patientReference = csvHelper.createPatientReference(patientId);
        if (referralRequestBuilder.isIdMapped()) {
            patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
        }
        referralRequestBuilder.setPatient(patientReference, patientId);

        CsvCell dateRecorded = parser.getDateEventRecorded();
        if (!dateRecorded.isEmpty()) {
            referralRequestBuilder.setRecordedDate(dateRecorded.getDateTime(), dateRecorded);
        }

        CsvCell referralDate = parser.getDateEvent();
        if (!referralDate.isEmpty()) {
            DateTimeType dateTimeType = new DateTimeType(referralDate.getDateTime());
            referralRequestBuilder.setDate(dateTimeType, referralDate);
        }

        CsvCell profileIdRecordedByCell = parser.getIDProfileEnteredBy();
        Reference recordedByReference = csvHelper.createPractitionerReferenceForProfileId(profileIdRecordedByCell);
        if (recordedByReference != null) {
            if (referralRequestBuilder.isIdMapped()) {
                recordedByReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(recordedByReference, fhirResourceFiler);
            }
            referralRequestBuilder.setRecordedBy(recordedByReference, profileIdRecordedByCell);
        }

        CsvCell referrerProfileIdCell = parser.getIDReferrer();
        CsvCell staffMemberIdDoneByCell = parser.getIDDoneBy();
        CsvCell orgIdDoneAtCell = parser.getIDOrganisationDoneAt();

        //the referral file has a specific profile ID field for who is performing the referral, so use that if possible
        //and fall back and use the staff ID & org if not
        if (!TppCsvHelper.isEmptyOrNegative(referrerProfileIdCell)) {
            Reference staffReference = csvHelper.createPractitionerReferenceForProfileId(referrerProfileIdCell);
            if (referralRequestBuilder.isIdMapped()) {
                staffReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(staffReference, fhirResourceFiler);
            }
            referralRequestBuilder.setRequester(staffReference, profileIdRecordedByCell);

        } else {
            Reference practitionerReference = csvHelper.createPractitionerReferenceForStaffMemberId(staffMemberIdDoneByCell, orgIdDoneAtCell);
            if (practitionerReference != null) {
                if (referralRequestBuilder.isIdMapped()) {
                    practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                }
                referralRequestBuilder.setRequester(practitionerReference, staffMemberIdDoneByCell, orgIdDoneAtCell);
            }
        }

        //NOTE: the TPP TypeOfReferral and ServiceOffered essentially represent the same concept -
        //a high-level descriptor of the type of the service being referred to (e.g. Hospital or Physiotherapy).
        //Because they're VERY rarely used in SystmOne and represent similar concepts, both values, if present,
        //will be stored in a new extension
        List<String> recipientServiceTypes = new ArrayList<>();

        CsvCell serviceOffered = parser.getServiceOffered();
        TppConfigListOption recipientServiceOffered = csvHelper.lookUpTppConfigListOption(serviceOffered);
        if (recipientServiceOffered != null && !Strings.isNullOrEmpty(recipientServiceOffered.getListOptionName())) {
            String recipientString = recipientServiceOffered.getListOptionName();
            referralRequestBuilder.setRecipientServiceOffered(recipientString, serviceOffered);
        }

        //note do this even if empty, since we may be updating an existing referral resource and want to clear the field
        String recipientServiceType = String.join(", ", recipientServiceTypes);
        referralRequestBuilder.setRecipientServiceType(recipientServiceType, serviceOffered);

        //the TPP referral reason is really the objective of the referral
        //e.g. Advice/Consultation or Advice and Support
        CsvCell reasonCell = parser.getPrimaryReason();
        TppConfigListOption reasonLookup = csvHelper.lookUpTppConfigListOption(reasonCell);
        if (reasonLookup != null) {
            //not going to attempt to map the term to the ReferralType values we have,
            //since there are 1000+ possible options in TPP, so we really need the Information Model to support this properly
            String term = reasonLookup.getListOptionName();
            referralRequestBuilder.setTypeFreeText(term, reasonCell);
        }

        //clear down any priority already on the resource
        if (referralRequestBuilder.hasCodeableConcept(CodeableConceptBuilder.Tag.Referral_Request_Priority)) {
            referralRequestBuilder.removeCodeableConcept(CodeableConceptBuilder.Tag.Referral_Request_Priority, null);
        }

        CsvCell referralPriorityCell = parser.getUrgency();
        TppConfigListOption priorityLookup = csvHelper.lookUpTppConfigListOption(referralPriorityCell);
        if (priorityLookup != null) {
            String desc = priorityLookup.getListOptionName();

            CodeableConceptBuilder ccb = new CodeableConceptBuilder(referralRequestBuilder, CodeableConceptBuilder.Tag.Referral_Request_Priority, true);

            //always carry over the text
            ccb.setText(desc, referralPriorityCell);

            //see if we've mapped it to a valueset value
            ReferralPriority mappedPriority = convertPriority(desc);
            if (mappedPriority != null) {
                ccb.addCoding(mappedPriority.getSystem()); //the URL for the priority system
                ccb.setCodingCode(mappedPriority.getCode());
                ccb.setCodingDisplay(mappedPriority.getDescription());
            }
            //SD-308 - don't bother trying to keep mapping all priorities to the same set and just leave to the IM to handle on the way out
            /*else {
                TransformWarnings.log(LOG, csvHelper, "Unmapped TPP referral priority {} {}", referralPriorityCell, desc);
            }*/
        }

        //we may have retrieved the Resource from the DB, so clear out any existing codeable concept
        if (referralRequestBuilder.hasCodeableConcept(CodeableConceptBuilder.Tag.Referral_Request_Service)) {
            referralRequestBuilder.removeCodeableConcept(CodeableConceptBuilder.Tag.Referral_Request_Service, null);
        }

        //        It has been agreed with NWL that to
        //        facilitate this data passing to the subscriber process all inbound referrals will be hard
        //        coded with the concept: Referral to service (XaAeO), which is mapped to the snomed code 306206005.
        CsvCell hardCodedSNOMEDDiagnosisCell = CsvCell.factoryDummyWrapper("306206005");
        CsvCell ctv3DiagnosisCell = CsvCell.factoryDummyWrapper("XaAeO");

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(referralRequestBuilder, CodeableConceptBuilder.Tag.Referral_Request_Service);
        TppCodingHelper.addCodes(codeableConceptBuilder, hardCodedSNOMEDDiagnosisCell, null, ctv3DiagnosisCell, null);


        // set consultation/encounter reference
        CsvCell eventId = parser.getIDEvent();
        if (!eventId.isEmpty()) {

            Reference eventReference = csvHelper.createEncounterReference(eventId);
            if (referralRequestBuilder.isIdMapped()) {
                eventReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(eventReference, fhirResourceFiler);
            }
            referralRequestBuilder.setEncounter(eventReference, eventId);
        }

        //status
        List<ReferralStatusRecord> statusesForReferral = csvHelper.getReferralStatusCache().getStatusesForReferral(referralIdCell, referralRequestBuilder);
        TppReferralStatusCache.addReferralStatuses(statusesForReferral, referralRequestBuilder);

        boolean mapIds = !referralRequestBuilder.isIdMapped();
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), mapIds, referralRequestBuilder);
    }

    private static ReferralPriority convertPriority(String priority) {

        if (priority.toLowerCase().contains("routine")) {
            return ReferralPriority.ROUTINE;

        } else if (priority.toLowerCase().contains("urgent")) {
            return ReferralPriority.URGENT;

        } else if (priority.toLowerCase().contains("two week")
                || priority.toLowerCase().contains("2 week")
                || priority.toLowerCase().contains("Two-Week Wait")) {
            return ReferralPriority.TWO_WEEK_WAIT;

        } else if (priority.toLowerCase().contains("soon")) {
            return ReferralPriority.SOON;

        } else if (priority.toLowerCase().contains("private")) {
            return ReferralPriority.PRIVATE;

        } else {
            return null;
        }
    }
}
