package org.endeavourhealth.transform.tpp.csv.transforms.referral;

import org.endeavourhealth.common.fhir.schema.ReferralPriority;
import org.endeavourhealth.common.fhir.schema.ReferralType;
import org.endeavourhealth.core.database.dal.publisherTransform.models.TppMappingRef;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.ReferralRequestBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisDateTimeHelper;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.ReferralRequestResourceCache;
import org.endeavourhealth.transform.tpp.csv.schema.referral.SRReferralOut;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Reference;

import java.util.Map;

public class SRReferralOutTransformer {

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

        ReferralRequestBuilder referralRequestBuilder
                = ReferralRequestResourceCache.getReferralBuilder(referralOutId, patientId, csvHelper, fhirResourceFiler);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        referralRequestBuilder.setPatient(patientReference, patientId);

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {
            referralRequestBuilder.setRecordedDate(dateRecored.getDate(), dateRecored);
        }

        CsvCell referralDate = parser.getDateEvent();
        DateTimeType dateTimeType = EmisDateTimeHelper.createDateTimeType(referralDate.getDate(), "YMD" );
        if (dateTimeType != null) {
            referralRequestBuilder.setDate(dateTimeType, referralDate);
        }

        CsvCell recordedBy = parser.getIDProfileEnteredBy();
        //TODO:  this links to SRStaffMemberProfile -> how get staff reference?

        CsvCell requestedByStaff = parser.getIDDoneBy();
        if (!recordedBy.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReference(requestedByStaff);
            referralRequestBuilder.setRequester(practitionerReference, recordedBy);
        }

        CsvCell requestedByOrg = parser.getIDOrganisationDoneAt();
        if (!requestedByOrg.isEmpty()) {
            //TODO: there can only be a single requester
        }

        CsvCell referralType = parser.getTypeOfReferral();
        if (!referralType.isEmpty() && referralType.getLong()>0) {

            TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(referralType.getLong());
            if(!tppMappingRef.getMappedTerm().isEmpty()) {
                ReferralType type = convertReferralType(tppMappingRef.getMappedTerm());
                if (type != null) {
                    referralRequestBuilder.setType(type, referralType);
                }
            }
        }

        CsvCell reason = parser.getReason();
        if (!reason.isEmpty() && reason.getLong()>0) {
            //TODO:  lookup SRConfigureListOption
        }

        CsvCell serviceOffered = parser.getServiceOffered();
        if (!serviceOffered.isEmpty() && serviceOffered.getLong()>0) {
            //TODO:  lookup SRConfigureListOption
        }


        CsvCell referralPriority = parser.getUrgency();
        if (!referralPriority.isEmpty()) {

            //TODO:  lookup SRConfigureListOption

            ReferralPriority priority = convertPriority(referralPriority.getString());
            if (priority != null) {
                referralRequestBuilder.setPriority(priority, referralPriority);
            } else {
                referralRequestBuilder.setPriorityFreeText(referralPriority.getString(), referralPriority);
            }
        }

        CsvCell referralPrimaryDiagnosisCode = parser.getPrimaryDiagnosis();
        //TODO: Ctv3 mapped to Snomed

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

        CsvCell referralParentEvent = parser.getIDEvent();
        if (!referralParentEvent.isEmpty()) {

            //TODO: how get event type to create reference, assume Problem ?
            Reference eventReference = csvHelper.createConditionReference (referralParentEvent, patientId);
            referralRequestBuilder.setParentResource(eventReference, referralParentEvent);
        }

    }

    private static ReferralPriority convertPriority(String priority) throws Exception {

//        if (urgency.equalsIgnoreCase("Routine")) {
//            return ReferralPriority.ROUTINE;
//
//        } else if (urgency.equalsIgnoreCase("Urgent")) {
//            return ReferralPriority.URGENT;
//
//        } else if (urgency.equalsIgnoreCase("2 Week Wait")) {
//            return ReferralPriority.TWO_WEEK_WAIT;
//
//        } else if (urgency.equalsIgnoreCase("Soon")) {
//            return ReferralPriority.SOON;
//
//        } else {
            return null;
//        }
    }

    private static Boolean recipientIsPerson (Long recipientTypeId, TppCsvHelper csvHelper) throws Exception {

        TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(recipientTypeId);
        String term = tppMappingRef.getMappedTerm();

        if (term.toLowerCase().startsWith("organisation")) {
            return false;
        }

        return true;
    }

    private static ReferralType convertReferralType(String type) throws Exception {

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
