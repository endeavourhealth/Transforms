package org.endeavourhealth.transform.emis.emisopen.transforms.admin;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.emis.emisopen.EmisOpenHelper;
import org.endeavourhealth.transform.emis.emisopen.schema.eommedicalrecord38.MedicalRecordType;
import org.endeavourhealth.transform.emis.emisopen.schema.eommedicalrecord38.PersonCategoryType;
import org.endeavourhealth.transform.emis.emisopen.schema.eommedicalrecord38.PersonType;
import org.endeavourhealth.transform.emis.emisopen.transforms.common.EmisOpenAddressConverter;
import org.hl7.fhir.instance.model.*;

import java.util.ArrayList;
import java.util.List;

public class PractitionerTransformer
{
    public static void transform(MedicalRecordType medicalRecordType, String organisationGuid, List<Resource> resources) throws TransformException
    {
        for (PersonType personType : medicalRecordType.getPeopleList().getPerson())
            resources.add(createPractitioner(personType, organisationGuid));
    }

    private static Practitioner createPractitioner(PersonType personType, String organisationGuid) throws TransformException
    {
        Practitioner practitioner = new Practitioner();
        practitioner.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_PRACTITIONER));

        practitioner.setId(personType.getGUID());

        String prescribingCode = personType.getPrescriptionCode();
        String nationalCode = personType.getNationalCode();

        String gmcCode = null;
        if (personType.getGmcCode() != null) {
            gmcCode = personType.getGmcCode().toString();
        }
        List<Identifier> identifiers = createIdentifiers(gmcCode, nationalCode, prescribingCode);
        //List<Identifier> identifiers = createIdentifiers(personType.getGmcCode().toString(), personType.getNationalCode(), personType.getPrescriptionCode());

        for (Identifier identifier : identifiers)
            practitioner.addIdentifier(identifier);

        practitioner.setName(NameHelper.convert(
                personType.getFirstNames(),
                personType.getLastName(),
                personType.getTitle()));

        if (personType.getAddress() != null)
            EmisOpenAddressConverter.convert(personType.getAddress(), Address.AddressUse.WORK);

        List<ContactPoint> contactPoints = ContactPointHelper.createWorkContactPoints(personType.getTelephone1(), personType.getTelephone2(), personType.getFax(), personType.getEmail());

        for (ContactPoint contactPoint : contactPoints)
            practitioner.addTelecom(contactPoint);

        Practitioner.PractitionerPractitionerRoleComponent practitionerPractitionerRoleComponent = new Practitioner.PractitionerPractitionerRoleComponent();
        practitionerPractitionerRoleComponent.setManagingOrganization(EmisOpenHelper.createOrganisationReference(organisationGuid));

        PersonCategoryType category = personType.getCategory();
        if (category != null
                && !Strings.isNullOrEmpty(category.getDescription())) {
            CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(category.getDescription());
            practitionerPractitionerRoleComponent.setRole(codeableConcept);
        }
        //practitionerPractitionerRoleComponent.setRole(new CodeableConcept().setText(personType.getCategory().getDescription()));

        return practitioner;
    }

    private static List<Identifier> createIdentifiers(String gmcNumber, String doctorIndexNumber, String gmpPpdCode)
    {
        List<Identifier> identifiers = new ArrayList<>();

        if (StringUtils.isNotBlank(gmcNumber))
        {
            identifiers.add(new Identifier()
                            .setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_GMC_NUMBER)
                            .setValue(gmcNumber));
        }

        if (StringUtils.isNotBlank(doctorIndexNumber))
        {
            identifiers.add(new Identifier()
                    .setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_DOCTOR_INDEX_NUMBER)
                    .setValue(doctorIndexNumber));
        }

        if (StringUtils.isNotBlank(gmpPpdCode))
        {
            identifiers.add(new Identifier()
                    .setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_GMP_PPD_CODE)
                    .setValue(gmpPpdCode));
        }

        return identifiers;
    }
}
