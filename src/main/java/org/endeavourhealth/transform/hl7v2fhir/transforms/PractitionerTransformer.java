package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.v23.datatype.HD;
import ca.uhn.hl7v2.model.v23.datatype.ST;
import ca.uhn.hl7v2.model.v23.datatype.XCN;
import ca.uhn.hl7v2.model.v23.segment.OBR;
import ca.uhn.hl7v2.model.v23.segment.PV1;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.NameBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerBuilder;
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
     * @param doctor
     * @param practitioner
     * @return
     * @throws Exception
     */
    public static Practitioner transformPV1ToPractitioner(XCN[] doctor, Practitioner practitioner) throws Exception {
        practitioner.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_PRACTITIONER));
        practitioner.setActive(true);

        if(doctor != null && doctor.length > 0) {
            ST idNum = doctor[0].getIDNumber();
            ST familyName = doctor[0].getFamilyName();
            ST givenName = doctor[0].getGivenName();
            HD assigningAuthority = doctor[0].getAssigningAuthority();

            Identifier identifier = new Identifier();
            identifier.setValue(String.valueOf(idNum));
            identifier.setSystem("http://endeavourhealth.org/fhir/Identifier/gmc-number");
            practitioner.addIdentifier(identifier);

            practitioner.setId(String.valueOf(idNum));
            HumanName humanName = new HumanName();
            humanName.setUse(HumanName.NameUse.USUAL);
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