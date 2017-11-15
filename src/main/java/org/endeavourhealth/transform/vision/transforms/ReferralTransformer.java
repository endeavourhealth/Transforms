package org.endeavourhealth.transform.vision.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.schema.ReferralPriority;
import org.endeavourhealth.common.fhir.schema.ReferralType;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.schema.AbstractCsvParser;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.endeavourhealth.transform.vision.schema.Referral;
import org.hl7.fhir.instance.model.Meta;
import org.hl7.fhir.instance.model.ReferralRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

public class ReferralTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ReferralTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 VisionCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Referral.class);
        while (parser.nextRecord()) {

            try {
                createResource((Referral)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    private static void createResource(Referral parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       VisionCsvHelper csvHelper) throws Exception {

        ReferralRequest fhirReferral = new ReferralRequest();
        fhirReferral.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_REFERRAL_REQUEST));

        String observationID = parser.getObservationID();
        String patientID = parser.getPatientID();

        VisionCsvHelper.setUniqueId(fhirReferral, patientID, observationID);

        fhirReferral.setPatient(csvHelper.createPatientReference(patientID));

        Date referralDate = parser.getReferralDate();
        fhirReferral.setDate(referralDate);

        //TODO: check referral urgency
//        String urgency = parser.getReferralUrgency();
//        if (!Strings.isNullOrEmpty(urgency)) {
//            ReferralPriority fhirPriority = convertUrgency(urgency);
//            if (fhirPriority != null) {
//                fhirReferral.setPriority(CodeableConceptHelper.createCodeableConcept(fhirPriority));
//            } else {
//                //if the CSV urgency couldn't be mapped to a FHIR priority, then we can use free-text
//                LOG.warn("Unmapped Emis referral priority {}", urgency);
//                fhirReferral.setPriority(CodeableConceptHelper.createCodeableConcept(urgency));
//            }
//        }

        String referralType = parser.getReferralType();
        if (!Strings.isNullOrEmpty(referralType)) {
            ReferralType type = convertReferralType(referralType);
            if (type != null) {
                fhirReferral.setType(CodeableConceptHelper.createCodeableConcept(type));
            } else {
                LOG.warn("Unmapped Vision referral type {}", referralType);
                fhirReferral.setType(CodeableConceptHelper.createCodeableConcept(referralType));
            }
        }

        String recipientOrgID = parser.getReferralDestOrgID();
        if (!Strings.isNullOrEmpty(recipientOrgID)) {
            fhirReferral.addRecipient(csvHelper.createOrganisationReference(recipientOrgID));
        }


        //TODO:// Encounter link  - The link value is pre-fixed with E  (need example) for an Encounter link
        String [] links = parser.getLinks().split("|");
//        String consultationID = EncounterLinks|    //map to an encounterId
//        if (!Strings.isNullOrEmpty(consultationID)) {
//            fhirObservation.setEncounter(csvHelper.createEncounterReference(consultationID, patientID));
//        }

        //addDocumentExtension(fhirReferral, parser);

        //unlike other resources, we don't save the Referral immediately, as there's data we
        //require on the corresponding row in the Observation file. So cache in the helper
        //and we'll finish the job when we get to that.
        csvHelper.cacheReferral(observationID, patientID, fhirReferral);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientID, fhirReferral);
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
