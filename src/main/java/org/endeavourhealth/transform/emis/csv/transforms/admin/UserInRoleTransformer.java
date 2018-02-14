package org.endeavourhealth.transform.emis.csv.transforms.admin;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.NameBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisAdminCacheFiler;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.admin.UserInRole;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Reference;

import java.util.Date;
import java.util.Map;

public class UserInRoleTransformer {

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        EmisAdminCacheFiler adminCacheFiler = new EmisAdminCacheFiler(csvHelper.getDataSharingAgreementGuid());

        AbstractCsvParser parser = parsers.get(UserInRole.class);
        while (parser.nextRecord()) {

            try {
                createResource((UserInRole)parser, fhirResourceFiler, csvHelper, adminCacheFiler);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

        adminCacheFiler.close();
    }

    private static void createResource(UserInRole parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper,
                                       EmisAdminCacheFiler adminCacheFiler) throws Exception {

        PractitionerBuilder practitionerBuilder = new PractitionerBuilder();

        CsvCell userInRoleGuid = parser.getUserInRoleGuid();
        practitionerBuilder.setId(userInRoleGuid.getString(), userInRoleGuid);

        CsvCell title = parser.getTitle();
        CsvCell givenName = parser.getGivenName();
        CsvCell surname = parser.getSurname();

        NameBuilder nameBuilder = new NameBuilder(practitionerBuilder);
        nameBuilder.beginName(HumanName.NameUse.OFFICIAL);
        nameBuilder.addPrefix(title.getString(), title);
        nameBuilder.addGiven(givenName.getString(), givenName);
        nameBuilder.addFamily(surname.getString(), surname);

        //need to call this to generate the role in the practitioner, as all the following fields are set on that
        practitionerBuilder.addRole();

        CsvCell startDate = parser.getContractStartDate();
        if (!startDate.isEmpty()) {
            Date date = startDate.getDate();
            practitionerBuilder.setRoleStartDate(date, startDate);
        }

        CsvCell endDate = parser.getContractEndDate();
        if (!endDate.isEmpty()) {
            Date date = endDate.getDate();
            practitionerBuilder.setRoleEndDate(date, endDate);
        }

        CsvCell orgUuid = parser.getOrganisationGuid();
        Reference organisationReference = csvHelper.createOrganisationReference(orgUuid);
        practitionerBuilder.setRoleManagingOrganisation(organisationReference, orgUuid);

        CsvCell roleName = parser.getJobCategoryName();
        if (!roleName.isEmpty()) {
            practitionerBuilder.setRoleName(roleName.getString(), roleName);
        }

        CsvCell roleCode = parser.getJobCategoryCode();
        if (!roleCode.isEmpty()) {
            practitionerBuilder.setRoleCode(roleCode.getString(), roleCode);
        }

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), practitionerBuilder);

        //this resource exists in our admin resource cache, so we can populate the
        //main database when new practices come on, so we need to update that too
        adminCacheFiler.saveAdminResourceToCache(parser.getCurrentState(), practitionerBuilder);
    }

    /*private static void createResource(UserInRole parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper,
                                       EmisAdminCacheFiler adminCacheFiler) throws Exception {

        Practitioner fhirPractitioner = new Practitioner();
        fhirPractitioner.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_PRACTITIONER));

        String userInRoleGuid = parser.getUserInRoleGuid();
        fhirPractitioner.setId(userInRoleGuid);

        String title = parser.getTitle();
        String givenName = parser.getGivenName();
        String surname = parser.getSurname();

        //the sample data contains users with a given name but no surname. FHIR requires all names
        //to have a surname, so treat the sole given name as the surname
        if (Strings.isNullOrEmpty(surname)) {
            surname = givenName;
            givenName = "";
        }

        //in the EMIS test pack, we have at least one record with no name details at all, so need to handle it
        if (Strings.isNullOrEmpty(surname)) {
            surname = "Unknown";
        }

        fhirPractitioner.setName(NameConverter.convert(givenName, surname, title));

        Date startDate = parser.getContractStartDate();
        Date endDate = parser.getContractEndDate();
        Period fhirPeriod = PeriodHelper.createPeriod(startDate, endDate);
        boolean active = PeriodHelper.isActive(fhirPeriod);

        fhirPractitioner.setActive(active);

        Practitioner.PractitionerPractitionerRoleComponent fhirRole = fhirPractitioner.addPractitionerRole();

        fhirRole.setPeriod(fhirPeriod);

        String orgUuid = parser.getOrganisationGuid();
        fhirRole.setManagingOrganization(csvHelper.createOrganisationReference(orgUuid));

        String roleName = parser.getJobCategoryName();
        String roleCode = parser.getJobCategoryCode();
        fhirRole.setRole(CodeableConceptHelper.createCodeableConcept(FhirValueSetUri.VALUE_SET_JOB_ROLE_CODES, roleName, roleCode));

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), fhirPractitioner);

        //this resource exists in our admin resource cache, so we can populate the
        //main database when new practices come on, so we need to update that too
        adminCacheFiler.saveAdminResourceToCache(parser.getCurrentState(), fhirPractitioner);
    }*/
}
