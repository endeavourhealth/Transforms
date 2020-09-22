package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.v23.datatype.HD;
import ca.uhn.hl7v2.model.v23.datatype.ST;
import ca.uhn.hl7v2.model.v23.datatype.XCN;
import ca.uhn.hl7v2.model.v23.segment.OBR;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.NameBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerBuilder;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Practitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PractitionerTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(PractitionerTransformer.class);

    /**
     *
     * @param doctor
     * @param practitioner
     * @return
     * @throws Exception
     */
    public static PractitionerBuilder transformPV1ToPractitioner(XCN[] doctor, PractitionerBuilder practitioner) throws Exception {
        //practitioner.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_PRACTITIONER));
        practitioner.setActive(true);

        if(doctor != null && doctor.length > 0) {
            ST idNum = doctor[0].getIDNumber();
            ST familyName = doctor[0].getFamilyName();
            ST givenName = doctor[0].getGivenName();
            HD assigningAuthority = doctor[0].getAssigningAuthority();

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitioner);
            identifierBuilder.setSystem("http://endeavourhealth.org/fhir/Identifier/gmc-number");
            identifierBuilder.setValue(String.valueOf(idNum));

            practitioner.setId(String.valueOf(idNum));
            NameBuilder nameBuilder = new NameBuilder(practitioner);
            nameBuilder.setUse(HumanName.NameUse.USUAL);
            nameBuilder.addGiven(String.valueOf(givenName));
            nameBuilder.addFamily(String.valueOf(familyName));
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