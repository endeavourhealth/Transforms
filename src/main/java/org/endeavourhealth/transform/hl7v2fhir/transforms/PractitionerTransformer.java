package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.v23.datatype.HD;
import ca.uhn.hl7v2.model.v23.datatype.ST;
import ca.uhn.hl7v2.model.v23.datatype.XCN;
import ca.uhn.hl7v2.model.v23.segment.PV1;
import org.endeavourhealth.common.fhir.FhirProfileUri;
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
            ST familyNameCd = consultingDoctor[0].getFamilyName();
            ST givenNameCd = consultingDoctor[0].getGivenName();
            HD assigningAuthorityCd = consultingDoctor[0].getAssigningAuthority();

            Identifier identifierCd = new Identifier();
            identifierCd.setValue(String.valueOf(idNumCd));
            identifierCd.setSystem("http://endeavourhealth.org/fhir/Identifier/gmc-number");
            practitioner.addIdentifier(identifierCd);
            practitioner.setId(String.valueOf(idNumCd));
        }

        return practitioner;
    }

}