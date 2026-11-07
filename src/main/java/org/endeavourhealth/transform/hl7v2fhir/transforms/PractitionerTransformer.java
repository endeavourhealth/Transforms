package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.v23.datatype.ST;
import ca.uhn.hl7v2.model.v23.datatype.XCN;
import ca.uhn.hl7v2.model.v23.segment.OBR;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformConstant;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.hl7v2fhir.helpers.AcuteHL7Helper;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Practitioner;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;

public class PractitionerTransformer {

    /**
     *
     * @param doctor
     * @param practitioner
     * @return
     * @throws Exception
     */
    public static PractitionerBuilder transformToPractitioner(XCN[] doctor, PractitionerBuilder practitioner) throws Exception {

        if(practitioner!=null && !practitioner.toString().isEmpty()) {
            practitioner.setActive(true);

            if (doctor != null && doctor.length > 0) {
                ST idNum = doctor[0].getIDNumber();
                ST familyName = doctor[0].getFamilyName();
                ST givenName = doctor[0].getGivenName();
                ST prefix = doctor[0].getPrefixEgDR();
                //HD assigningAuthority = doctor[0].getAssigningAuthority();

                IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitioner);
                identifierBuilder.setSystem("http://endeavourhealth.org/fhir/Identifier/gmc-number");
                identifierBuilder.setValue(String.valueOf(idNum));

                practitioner.setId(String.valueOf(idNum));
                NameBuilder nameBuilder = new NameBuilder(practitioner);
                nameBuilder.setUse(HumanName.NameUse.USUAL);
                nameBuilder.addGiven(String.valueOf(givenName));
                nameBuilder.addFamily(String.valueOf(familyName));
                nameBuilder.addPrefix(String.valueOf(prefix));
            }
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

    public static void addPractitioner(PatientBuilder patientBuilder, Reference reference) {
        //add in additional extended data as Parameters resource with additional extension
        ContainedParametersBuilder containedParametersBuilder = new ContainedParametersBuilder(patientBuilder);
        containedParametersBuilder.removeContainedParameters();
        containedParametersBuilder.addParameter(TransformConstant.PARAMETER_GP_PRACTITIONER_ID,
                reference);
    }

    public static PractitionerBuilder createAcutePractitioner(FhirResourceFiler fhirResourceFiler, XCN[] doctor, OrganizationBuilder organizationBuilder) throws Exception {
        PractitionerBuilder practitionerBuilder = new PractitionerBuilder();
        if (doctor != null && doctor.length > 0) {
            practitionerBuilder = PractitionerTransformer.transformToPractitioner(doctor, practitionerBuilder);
            if (practitionerBuilder.getResourceId() != null && organizationBuilder.getResourceId() != null) {
                PractitionerRoleBuilder roleBuilderConsulting = new PractitionerRoleBuilder(practitionerBuilder);
                roleBuilderConsulting.setRoleManagingOrganisation(AcuteHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
                fhirResourceFiler.saveAdminResource(null, practitionerBuilder);
            }
        }
        return practitionerBuilder;
    }
}