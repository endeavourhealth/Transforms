package org.endeavourhealth.transform.emis.csv.transforms.admin;

import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.NameBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerRoleBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisAdminCacheFiler;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.admin.UserInRole;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Reference;

import java.util.Date;
import java.util.Map;

public class UserInRoleTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        EmisAdminCacheFiler adminCacheFiler = new EmisAdminCacheFiler(csvHelper.getDataSharingAgreementGuid());

        AbstractCsvParser parser = parsers.get(UserInRole.class);
        while (parser != null && parser.nextRecord()) {

            try {
                createResource((UserInRole)parser, fhirResourceFiler, csvHelper, adminCacheFiler);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

        adminCacheFiler.close();

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
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
        nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
        nameBuilder.addPrefix(title.getString(), title);
        nameBuilder.addGiven(givenName.getString(), givenName);
        nameBuilder.addFamily(surname.getString(), surname);

        //need to call this to generate the role in the practitioner, as all the following fields are set on that
        PractitionerRoleBuilder roleBuilder = new PractitionerRoleBuilder(practitionerBuilder);

        CsvCell startDate = parser.getContractStartDate();
        if (!startDate.isEmpty()) {
            Date date = startDate.getDate();
            roleBuilder.setRoleStartDate(date, startDate);
        }

        CsvCell endDate = parser.getContractEndDate();
        if (!endDate.isEmpty()) {
            Date date = endDate.getDate();
            roleBuilder.setRoleEndDate(date, endDate);
        }

        //after doing the start and end dates, call this to calculate the active state from them
        practitionerBuilder.calculateActiveFromRoles();

        CsvCell orgUuid = parser.getOrganisationGuid();
        Reference organisationReference = csvHelper.createOrganisationReference(orgUuid);
        roleBuilder.setRoleManagingOrganisation(organisationReference, orgUuid);

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(roleBuilder, CodeableConceptBuilder.Tag.Practitioner_Role);
        codeableConceptBuilder.addCoding(FhirValueSetUri.VALUE_SET_JOB_ROLE_CODES);

        CsvCell roleName = parser.getJobCategoryName();
        if (!roleName.isEmpty()) {
            codeableConceptBuilder.setCodingDisplay(roleName.getString(), roleName);
        }

        CsvCell roleCode = parser.getJobCategoryCode();
        if (!roleCode.isEmpty()) {
            codeableConceptBuilder.setCodingCode(roleCode.getString(), roleCode);
        }

        //this resource exists in our admin resource cache, so we can populate the
        //main database when new practices come on, so we need to update that too
        adminCacheFiler.saveAdminResourceToCache(practitionerBuilder);

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), practitionerBuilder);
    }

}
