package org.endeavourhealth.transform.vision.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.schema.ReferralPriority;
import org.endeavourhealth.common.fhir.schema.ReferralType;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.ReferralRequestBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisDateTimeHelper;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.endeavourhealth.transform.vision.schema.Referral;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.endeavourhealth.transform.vision.transforms.JournalTransformer.extractEncounterLinkID;

public class ReferralTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ReferralTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
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
    }

    private static void createResource(Referral parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       VisionCsvHelper csvHelper) throws Exception {


        ReferralRequestBuilder referralRequestBuilder = new ReferralRequestBuilder();
        CsvCell referralID = parser.getReferralID();
        CsvCell patientID = parser.getPatientID();

        VisionCsvHelper.setUniqueId(referralRequestBuilder, patientID, referralID);

        Reference patientReference = csvHelper.createPatientReference(patientID);
        referralRequestBuilder.setPatient(patientReference, patientID);

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getAction().getString().equalsIgnoreCase("D")) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), referralRequestBuilder);
            return;
        }

        CsvCell referralDate = parser.getReferralDate();
        referralRequestBuilder.setDate(EmisDateTimeHelper.createDateTimeType(referralDate.getDate(), "YMD"), referralDate);

        CsvCell referralUserID = parser.getReferralUserID();
        if (!referralUserID.isEmpty()) {
            String cleanReferralUserID = csvHelper.cleanUserId(referralUserID.getString());
            referralRequestBuilder.setRequester(csvHelper.createPractitionerReference(cleanReferralUserID));
        }

        CsvCell referralType = parser.getReferralType();
        if (!referralType.isEmpty()) {
            ReferralType type = convertReferralType(referralType.getString());
            if (type != null) {
                referralRequestBuilder.setType(type);
            }
        }

        CsvCell recipientOrgID = parser.getReferralDestOrgID();
        if (!recipientOrgID.isEmpty()) {
            referralRequestBuilder.addRecipient(csvHelper.createOrganisationReference(recipientOrgID.getString()));
        }

        //set linked encounter
        String consultationID = extractEncounterLinkID(parser.getLinks().getString());
        if (!Strings.isNullOrEmpty(consultationID)) {
            referralRequestBuilder.setEncounter(csvHelper.createEncounterReference(consultationID, patientID.getString()));
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
