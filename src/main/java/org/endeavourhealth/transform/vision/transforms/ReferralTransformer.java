package org.endeavourhealth.transform.vision.transforms;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.common.fhir.schema.ReferralPriority;
import org.endeavourhealth.common.fhir.schema.ReferralType;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ResourceParser;
import org.endeavourhealth.transform.common.resourceBuilders.ReferralRequestBuilder;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.endeavourhealth.transform.vision.helpers.VisionDateTimeHelper;
import org.endeavourhealth.transform.vision.schema.Referral;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ReferralTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ReferralTransformer.class);

    private static Map<String, String> specialtyCodeMapCache = null;

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 VisionCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Referral.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((Referral) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createResource(Referral parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       VisionCsvHelper csvHelper) throws Exception {


        ReferralRequestBuilder referralRequestBuilder = new ReferralRequestBuilder();
        CsvCell referralIdCell = parser.getReferralID();
        CsvCell patientIdCell = parser.getPatientID();

        VisionCsvHelper.setUniqueId(referralRequestBuilder, patientIdCell, referralIdCell);

        Reference patientReference = csvHelper.createPatientReference(patientIdCell);
        referralRequestBuilder.setPatient(patientReference, patientIdCell);

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell actionCell = parser.getAction();
        if (actionCell.getString().equalsIgnoreCase("D")) {
            referralRequestBuilder.setDeletedAudit(actionCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), referralRequestBuilder);
            return;
        }

        CsvCell dateCell = parser.getReferralDate();
        DateTimeType dateTimeType = VisionDateTimeHelper.getDateTime(dateCell, null);
        if (dateTimeType != null) {
            referralRequestBuilder.setDate(dateTimeType, dateCell);
        }

        CsvCell referralSenderUserId = parser.getReferralSenderUserId();
        if (!referralSenderUserId.isEmpty()) {
            String cleanReferralUserID = VisionCsvHelper.cleanUserId(referralSenderUserId.getString());
            Reference ref = csvHelper.createPractitionerReference(cleanReferralUserID);
            referralRequestBuilder.setRequester(ref, referralSenderUserId);
        }

        CsvCell referralTypeCell = parser.getReferralType();
        if (!referralTypeCell.isEmpty()) {
            ReferralType type = convertReferralType(referralTypeCell.getString());
            if (type != null) {
                referralRequestBuilder.setType(type, referralTypeCell);
            }
        }

        CsvCell recipientUserIdCell = parser.getReferralRecipientUserId();
        if (!recipientUserIdCell.isEmpty()) {
            Reference ref = csvHelper.createPractitionerReference(recipientUserIdCell.getString());
            referralRequestBuilder.addRecipient(ref, recipientUserIdCell);
        }

        //set linked encounter
        CsvCell linksCell = parser.getLinks();
        String consultationId = JournalTransformer.extractEncounterLinkId(linksCell.getString());
        if (!Strings.isNullOrEmpty(consultationId)) {
            Reference ref = csvHelper.createEncounterReference(consultationId, patientIdCell.getString());
            referralRequestBuilder.setEncounter(ref, linksCell);
        }

        CsvCell specialtyCell = parser.getSpecialty();
        if (!specialtyCell.isEmpty()) {
            //the specialty is a NHS Data Dictionary specialty code
            if (specialtyCodeMapCache == null) {
                specialtyCodeMapCache = ResourceParser.readCsvResourceIntoMap("DataDictionarySpecialtyCodes.csv", "Code", "Term", CSVFormat.DEFAULT.withHeader());
            }
            String term = specialtyCodeMapCache.get(specialtyCell.getString());
            if (term == null) {
                throw new Exception("Failed to find clinical specialty term for code [" + specialtyCell.getString() + "]");
            }
            referralRequestBuilder.setRecipientServiceType(term, specialtyCell);
        }

        //there are a few fields that are always empty, so we don't transform them. But validate that they ARE empty.
        CsvCell contractorCell = parser.getContractor();
        if (!contractorCell.isEmpty()) {
            throw new Exception("Referal CONTRACTOR cell not empty");
        }
        CsvCell contractCell = parser.getContract();
        if (!contractCell.isEmpty()) {
            throw new Exception("Referal CONTRACT cell not empty");
        }
        CsvCell actionDateCell = parser.getActionDate();
        if (!actionDateCell.isEmpty()) {
            throw new Exception("Referal ACTION_DATE cell not empty");
        }
        CsvCell unitCell = parser.getUnit();
        if (!unitCell.isEmpty()) {
            throw new Exception("Referal UNIT cell not empty");
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), referralRequestBuilder);
    }

    private static ReferralType convertReferralType(String type) throws Exception {

        switch (type) {
            case "O": return ReferralType.OUTPATIENT;       //OPD
            case "A": return ReferralType.ADMISSION;	    //Admission
            case "D": return ReferralType.DAY_CARE;         //Day-case
            case "I": return ReferralType.INVESTIGATION; 	//Investigation
            case "V": return ReferralType.COMMUNITY_CARE;   //Domiciliary visit
            default: return ReferralType.UNKNOWN;
        }
    }

    private static ReferralPriority convertUrgency(String urgency) throws Exception {

        if (urgency.equalsIgnoreCase("Routine")) {
            return ReferralPriority.ROUTINE;

        } else if (urgency.equalsIgnoreCase("Urgent")) {
            return ReferralPriority.URGENT;

        } else if (urgency.equalsIgnoreCase("2 Week Wait")) {
            return ReferralPriority.TWO_WEEK_WAIT;

        } else if (urgency.equalsIgnoreCase("Soon")) {
            return ReferralPriority.SOON;

        } else {
            return null;
        }
    }

}
