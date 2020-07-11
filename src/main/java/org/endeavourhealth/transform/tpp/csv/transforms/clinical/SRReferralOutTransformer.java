package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.schema.ReferralPriority;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppConfigListOption;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppCtv3Lookup;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMappingRef;
import org.endeavourhealth.core.terminology.SnomedCode;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ReferralRequestBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCodingHelper;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.helpers.cache.MedicalRecordStatusCacheObject;
import org.endeavourhealth.transform.tpp.csv.helpers.cache.ReferralStatusRecord;
import org.endeavourhealth.transform.tpp.csv.helpers.cache.TppRecordStatusCache;
import org.endeavourhealth.transform.tpp.csv.helpers.cache.TppReferralStatusCache;
import org.endeavourhealth.transform.tpp.csv.schema.referral.SRReferralOut;
import org.hl7.fhir.instance.model.*;
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

        CsvCell referralIdCell = parser.getRowIdentifier();
        CsvCell removeDataCell = parser.getRemovedData();


        ReferralRequestBuilder referralRequestBuilder = null;
        ReferralRequest referralRequest = (ReferralRequest)csvHelper.retrieveResource(referralIdCell.getString(), ResourceType.ReferralRequest);
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
        if (!TppCsvHelper.isEmptyOrNegative(profileIdRecordedBy)) {
            Reference staffReference = csvHelper.createPractitionerReferenceForProfileId(profileIdRecordedBy);
            if (referralRequestBuilder.isIdMapped()) {
                staffReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(staffReference, fhirResourceFiler);
            }
            referralRequestBuilder.setRecordedBy(staffReference, profileIdRecordedBy);
        }

        CsvCell referrerProfileIdCell = parser.getIDProfileReferrer();
        CsvCell staffMemberIdDoneByCell = parser.getIDDoneBy();
        CsvCell orgIdDoneAtCell = parser.getIDOrganisationDoneAt();

        //the referral file has a specific profile ID field for who is performing the referral, so use that if possible
        //and fall back and use the staff ID & org if not
        if (!TppCsvHelper.isEmptyOrNegative(referrerProfileIdCell)) {
            Reference staffReference = csvHelper.createPractitionerReferenceForProfileId(referrerProfileIdCell);
            if (referralRequestBuilder.isIdMapped()) {
                staffReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(staffReference, fhirResourceFiler);
            }
            referralRequestBuilder.setRequester(staffReference, profileIdRecordedBy);

        } else {
            Reference practitionerReference = csvHelper.createPractitionerReferenceForStaffMemberId(staffMemberIdDoneByCell, orgIdDoneAtCell);
            if (practitionerReference != null) {
                if (referralRequestBuilder.isIdMapped()) {
                    practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                }
                referralRequestBuilder.setRequester(practitionerReference, staffMemberIdDoneByCell);
            }
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

        CsvCell referralTypeCell = parser.getTypeOfReferral();
        //TPP type of referral is a high-level of the the service type being referred so
        TppMappingRef mapping = csvHelper.lookUpTppMappingRef(referralTypeCell);
        if (mapping != null) {
            String term = mapping.getMappedTerm();
            recipientServiceTypes.add(term);
        }

        //note do this even if empty, since we may be updating an existing referral resource and want to clear the field
        String recipientServiceType = String.join(", ", recipientServiceTypes);
        referralRequestBuilder.setRecipientServiceType(recipientServiceType, serviceOffered);

        //the TPP referral reason is really the objective of the referral
        //e.g. Advice/Consultation or Advice and Support
        CsvCell reasonCell = parser.getReason();
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

            } else {
                TransformWarnings.log(LOG, csvHelper, "Unmapped TPP referral priority {} {}", referralPriorityCell, desc);
            }
        }

        //we may have retrieved the Resource from the DB, so clear out any existing codeable concept
        if (referralRequestBuilder.hasCodeableConcept(CodeableConceptBuilder.Tag.Referral_Request_Service)) {
            referralRequestBuilder.removeCodeableConcept(CodeableConceptBuilder.Tag.Referral_Request_Service, null);
        }

        CsvCell snomedDiagnosisCell = parser.getSNOMEDPrimaryDiagnosis();
        CsvCell ctv3DiagnosisCell = parser.getPrimaryDiagnosis();

        if (!ctv3DiagnosisCell.isEmpty()
                || (snomedDiagnosisCell != null //snomed column not always present, so need null check
                && !TppCsvHelper.isEmptyOrNegative(snomedDiagnosisCell))) {

            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(referralRequestBuilder, CodeableConceptBuilder.Tag.Referral_Request_Service);
            TppCodingHelper.addCodes(codeableConceptBuilder, snomedDiagnosisCell, null, ctv3DiagnosisCell, null);
        }


        CsvCell recipientTypeCell = parser.getRecipientIDType();
        CsvCell recipientIdCell = parser.getRecipientID();
        if (!recipientTypeCell.isEmpty()
                && !recipientIdCell.isEmpty()) {

            //the recipient type gives us one of five types of recipient (GMS number, NMC number, Organisation ID, Other ID and Unknown)
            //however, analysis of live data has shown this doesn't correlate to any pattern in the recipient ID field,
            //which seems to be mostly a mess of free-text. So just generate a recipient desc from what we can
            //and carry it over as free-text.
            TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(recipientTypeCell);
            String desc = tppMappingRef.getMappedTerm();
            desc += ": ";
            desc += recipientIdCell.getString();

            referralRequestBuilder.setRecipientFreeText(desc, recipientIdCell, recipientTypeCell);
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
                || priority.toLowerCase().contains("2 week")) {
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
