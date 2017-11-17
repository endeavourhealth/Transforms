package org.endeavourhealth.transform.vision.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.common.fhir.NameConverter;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.schema.AbstractCsvParser;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.endeavourhealth.transform.vision.schema.Staff;
import org.hl7.fhir.instance.model.Meta;
import org.hl7.fhir.instance.model.Practitioner;

import java.util.Map;

public class StaffTransformer {

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 VisionCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Staff.class);
        while (parser.nextRecord()) {

            try {
                createResource((Staff)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    private static void createResource(Staff parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       VisionCsvHelper csvHelper) throws Exception {

        Practitioner fhirPractitioner = new Practitioner();
        fhirPractitioner.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_PRACTITIONER));

        String userID = parser.getUserID();
        fhirPractitioner.setId(userID);

        String title = parser.getTitle();
        String givenName = parser.getGivenName();
        String surname = parser.getSurname();

        //the sample data contains users with a given name but no surname. FHIR requires all names
        //to have a surname, so treat the sole given name as the surname
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

        //fhirPractitioner.setActive(true);  //assume active for vision?

        fhirRole.setRole(CodeableConceptHelper.createCodeableConcept(FhirValueSetUri.VALUE_SET_JOB_ROLE_CODES, roleName, roleCode));

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), fhirPractitioner);
    }


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
