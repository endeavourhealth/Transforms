package org.endeavourhealth.transform.emis.csv.transforms.admin;

import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.EmisLocationDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.EmisUserInRoleDalI;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.NameBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerRoleBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.admin.Location;
import org.endeavourhealth.transform.emis.csv.schema.admin.UserInRole;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Reference;

import java.util.Date;
import java.util.Map;

public class UserInRoleTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        UserInRole parser = (UserInRole)parsers.get(UserInRole.class);
        if (parser != null) {

            //iterate through the file to make sure it's audited and to log all the changed record IDs
            while (parser.nextRecord()) {
                CsvCell userInRoleGuidCell = parser.getUserInRoleGuid();
                csvHelper.getAdminHelper().addUserInRoleChanged(userInRoleGuidCell);
            }

            //the above will have audited the table, so now we can load the bulk staging table with our file
            String filePath = parser.getFilePath();
            Date dataDate = fhirResourceFiler.getDataDate();
            EmisUserInRoleDalI dal = DalProvider.factoryEmisUserInRoleDal();
            int fileId = parser.getFileAuditId().intValue();
            dal.updateStagingTable(filePath, dataDate, fileId);

            //call this to abort if we had any errors, during the above processing
            fhirResourceFiler.failIfAnyErrors();
        }
    }

    /*private static void createResource(UserInRole parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper) throws Exception {

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

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), practitionerBuilder);
    }
*/
}
