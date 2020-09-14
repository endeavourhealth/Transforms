package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.v23.datatype.HD;
import ca.uhn.hl7v2.model.v23.datatype.ST;
import ca.uhn.hl7v2.model.v23.datatype.XCN;
import ca.uhn.hl7v2.model.v23.group.ORU_R01_ORDER_OBSERVATION;
import ca.uhn.hl7v2.model.v23.segment.OBR;
import ca.uhn.hl7v2.model.v23.segment.PV1;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.NameBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerBuilder;
import org.endeavourhealth.transform.hl7v2fhir.helpers.ImperialHL7Helper;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Meta;
import org.hl7.fhir.instance.model.Practitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PractitionerTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(PractitionerTransformer.class);
    public static final String APPT_ID_SUFFIX = ":Appointment";

    /**
     *
     * @param pv1
     * @param practitioner
     * @return
     * @throws Exception
     */
    public static Practitioner transformPV1ToPractitioner(PV1 pv1, Practitioner practitioner) throws Exception {
        practitioner.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_PRACTITIONER));
        practitioner.setActive(true);

        /*XCN[] referringDoctor = pv1.getReferringDoctor();
        if(referringDoctor != null && referringDoctor.length > 0) {
            ST idNumRd = referringDoctor[0].getIDNumber();
            FN familyNameRd = referringDoctor[0].getFamilyName();
            ST givenNameRd = referringDoctor[0].getGivenName();
            HD assigningAuthorityRd = referringDoctor[0].getAssigningAuthority();

            Identifier identifierRd = new Identifier();
            identifierRd.setValue(String.valueOf(idNumRd));
            identifierRd.setSystem(String.valueOf(assigningAuthorityRd.getNamespaceID()));
            practitioner.addIdentifier(identifierRd);

            HumanName name = practitioner.getName();
            name.setUse(HumanName.NameUse.USUAL);
            name.setText(String.valueOf(givenNameRd));
            practitioner.setName(name);
        }

        XCN[] attendingDoctor = pv1.getAttendingDoctor();
        if(attendingDoctor != null && attendingDoctor.length > 0) {
            ST idNumAd = attendingDoctor[0].getIDNumber();
            FN familyNameAd = attendingDoctor[0].getFamilyName();
            ST givenNameAd = attendingDoctor[0].getGivenName();
            HD assigningAuthorityAd = attendingDoctor[0].getAssigningAuthority();

            Identifier identifierAd = new Identifier();
            identifierAd.setValue(String.valueOf(idNumAd));
            identifierAd.setSystem(String.valueOf(assigningAuthorityAd.getNamespaceID()));
            practitioner.addIdentifier(identifierAd);
        }*/

        XCN[] consultingDoctor = pv1.getConsultingDoctor();
        if(consultingDoctor != null && consultingDoctor.length > 0) {
            ST idNumCd = consultingDoctor[0].getIDNumber();
            ST familyName = consultingDoctor[0].getFamilyName();
            ST givenName = consultingDoctor[0].getGivenName();
            HD assigningAuthorityCd = consultingDoctor[0].getAssigningAuthority();

            Identifier identifierCd = new Identifier();
            identifierCd.setValue(String.valueOf(idNumCd));
            identifierCd.setSystem("http://endeavourhealth.org/fhir/Identifier/gmc-number");
            practitioner.addIdentifier(identifierCd);
            practitioner.setId(String.valueOf(idNumCd));
            HumanName humanName = new HumanName();
            humanName.setText(String.valueOf(familyName)+" "+String.valueOf(givenName));
            practitioner.setName(humanName);
        }

        return practitioner;
    }

    /**
     *
     * @param obr
     * @param practitioner
     * @param fhirResourceFiler
     * @throws Exception
     */
    public static void transformPathPractitioner(OBR obr, Practitioner practitioner, FhirResourceFiler fhirResourceFiler) throws Exception {
        XCN[] orderingProvider = obr.getOrderingProvider();
        ST familyName = orderingProvider[0].getFamilyName();
        ST givenName = orderingProvider[0].getGivenName();
        ST idNumber = orderingProvider[0].getIDNumber();

        PractitionerBuilder practitionerBuilder = null;
        if (practitioner == null) {
            practitionerBuilder = new PractitionerBuilder();
            practitionerBuilder.setId(idNumber.toString());

        } else {
            practitionerBuilder = new PractitionerBuilder(practitioner);
        }

        NameBuilder.removeExistingNames(practitionerBuilder);

        NameBuilder nameBuilder = new NameBuilder(practitionerBuilder);
        nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
        nameBuilder.addGiven(givenName.toString());
        nameBuilder.addFamily(familyName.toString());

        fhirResourceFiler.saveAdminResource(null, practitionerBuilder);
    }

}