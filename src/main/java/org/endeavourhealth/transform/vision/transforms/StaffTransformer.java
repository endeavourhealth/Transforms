package org.endeavourhealth.transform.vision.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.endeavourhealth.transform.vision.schema.Staff;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Reference;

import java.util.Map;

public class StaffTransformer {

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 VisionCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Staff.class);

        //if the Staff file has skipped a day then it might not be available
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((Staff) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createResource(Staff parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       VisionCsvHelper csvHelper) throws Exception {

        PractitionerBuilder practitionerBuilder = new PractitionerBuilder();
        CsvCell userID = parser.getUserID();
        practitionerBuilder.setId(userID.getString(), userID);

        CsvCell title = parser.getTitle();
        CsvCell givenName = parser.getGivenName();
        CsvCell surname = parser.getSurname();

        NameBuilder nameBuilder = new NameBuilder(practitionerBuilder);
        nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
        nameBuilder.addPrefix(title.getString(), title);
        nameBuilder.addGiven(givenName.getString(), givenName);
        nameBuilder.addFamily(surname.getString(), surname);

        PractitionerRoleBuilder roleBuilder = new PractitionerRoleBuilder(practitionerBuilder);
        CsvCell orgID = parser.getOrganisationID();
        Reference organisationReference = csvHelper.createOrganisationReference(orgID.getString());
        roleBuilder.setRoleManagingOrganisation(organisationReference, orgID);

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(roleBuilder, CodeableConceptBuilder.Tag.Practitioner_Role);
        codeableConceptBuilder.addCoding(FhirValueSetUri.VALUE_SET_JOB_ROLE_CODES);

        CsvCell roleCode = parser.getJobCategoryCode();
        if (!roleCode.isEmpty()) {
            codeableConceptBuilder.setCodingCode(roleCode.getString(), roleCode);
        }

        String roleName = getJobCategoryName(roleCode.getString());
        if (!roleName.isEmpty()) {
            codeableConceptBuilder.setCodingDisplay(roleName);   //don't pass in a cell as roleName was derived
        }

        CsvCell gmpCode = parser.getGMPCode();
        if (!gmpCode.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_GMP_PPD_CODE);
            identifierBuilder.setValue(gmpCode.getString(), gmpCode);
        }

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), practitionerBuilder);
    }

    /*private static void createResource(Staff parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       VisionCsvHelper csvHelper) throws Exception {

        Practitioner fhirPractitioner = new Practitioner();
        fhirPractitioner.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_PRACTITIONER));

        String userID = parser.getUserID();
        fhirPractitioner.setId(userID);

        String title = parser.getTitle();
        String givenName = parser.getGivenName();
        String surname = parser.getSurname();

        if (Strings.isNullOrEmpty(surname)) {
            surname = givenName;
            givenName = "";
        }

        if (Strings.isNullOrEmpty(surname)) {
            surname = "Unknown";
        }

        fhirPractitioner.setName(NameConverter.convert(givenName, surname, title));

        Practitioner.PractitionerPractitionerRoleComponent fhirRole = fhirPractitioner.addPractitionerRole();

        String orgID = parser.getOrganisationID();
        fhirRole.setManagingOrganization(csvHelper.createOrganisationReference(orgID));

        String roleCode = parser.getJobCategoryCode();
        String roleName = getJobCategoryName(roleCode);

        String gmpCode = parser.getGMPCode();
        if (!Strings.isNullOrEmpty(gmpCode)) {
            Identifier identifier = new Identifier()
                    .setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_GMP_PPD_CODE)
                    .setValue(gmpCode);
            fhirPractitioner.addIdentifier(identifier);
        }

        //fhirPractitioner.setActive(true);  //assume active for vision?

        fhirRole.setRole(CodeableConceptHelper.createCodeableConcept(FhirValueSetUri.VALUE_SET_JOB_ROLE_CODES, roleName, roleCode));

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), fhirPractitioner);
    }*/


    public static String getJobCategoryName(String jobCategoryCode) {

        switch (jobCategoryCode){
            case "A": return "Principal GP";
            case "B": return "Locum GP";
            case "C": return "GP Registrar";
            case "D": return "Other Practice staff";
            case "D06": return "Practice Nurse";
            case "D07": return "Dispenser";
            case "D08": return "Physiotherapist";
            case "D09": return "Chiropodist";
            case "D10": return "Interpreter /Link Worker";
            case "D11": return "Counsellor";
            case "D12": return "Osteopath";
            case "D13": return "Chiropractor";
            case "D14": return "Acupuncturist";
            case "D15": return "Homeopath";
            case "D16": return "Health Visitor";
            case "D17": return "District Nurse";
            case "D18": return "Community Psychiatric Nurse";
            case "D19": return "Mental Handicap Nurse";
            default: return "None";
        }
    }
}
