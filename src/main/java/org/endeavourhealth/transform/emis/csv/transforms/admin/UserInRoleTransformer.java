package org.endeavourhealth.transform.emis.csv.transforms.admin;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.EmisUserInRoleDalI;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.admin.UserInRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

public class UserInRoleTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(UserInRole.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        UserInRole parser = (UserInRole)parsers.get(UserInRole.class);
        if (parser != null) {

            /*
            //EMIS001_AWS:  delta_38206_Admin_UserInRole_20211124002215_2D106103-404D-4054-89B8-18F95932B092.csv
            //              delta_38211_Admin_UserInRole_20211125002954_2D106103-404D-4054-89B8-18F95932B092.csv
            //EMIS002_AWS:  delta_38206_Admin_UserInRole_20211124002215_93F5B33A-BD08-4B24-97A5-5B809CBF4C9C.csv
                            delta_38211_Admin_UserInRole_20211125002954_93F5B33A-BD08-4B24-97A5-5B809CBF4C9C.csv
            //EMIS003_AWS:  delta_38206_Admin_UserInRole_20211124002215_5AEE4A09-B9A6-411D-A13D-B9E88A9A3DBA.csv
                            delta_38211_Admin_UserInRole_20211125002954_5AEE4A09-B9A6-411D-A13D-B9E88A9A3DBA.csv
            //EMIS004_AWS:  delta_38206_Admin_UserInRole_20211124002215_B3FA020B-05FC-4196-A4DE-E91B70BEF1D2.csv
                            delta_38211_Admin_UserInRole_20211125002954_B3FA020B-05FC-4196-A4DE-E91B70BEF1D2.csv
            //EMIS005_AWS:  delta_38206_Admin_UserInRole_20211124002215_C28E1D77-5A54-455B-89D5-0BD6E7B23C8C.csv
                            delta_38211_Admin_UserInRole_20211125002954_C28E1D77-5A54-455B-89D5-0BD6E7B23C8C.csv
            //EMIS006_AWS:  delta_38206_Admin_UserInRole_20211124002215_66B86AD2-A450-4FFE-8E3D-64DDAFDF7227.csv
                            delta_38211_Admin_UserInRole_20211125002954_66B86AD2-A450-4FFE-8E3D-64DDAFDF7227.csv

               Plus these received as part of the unplanned bulk of the entire of EMIS003:

               bulk_38372_Admin_UserInRole_20211206001532_5AEE4A09-B9A6-411D-A13D-B9E88A9A3DBA.csv

               and EMIS005:

               bulk_38372_Admin_UserInRole_20211206001532_C28E1D77-5A54-455B-89D5-0BD6E7B23C8C.csv
             */
            //recieved new massive erroneous file for
            //EMIS004_AWS april 29th bulk: bulk_70670_Admin_UserInRole_20250429000003_B3FA020B-05FC-4196-A4DE-E91B70BEF1D2.csv

            //temporary change to exclude the massive erroneous file received via the EMIS AWS feed for 24th November
            //and 25th November and the two included in the unexpected bulk on 6th December.
            if (parser.getFilePath().contains("delta_38206_Admin_UserInRole_20211124002215_2D106103-404D-4054-89B8-18F95932B092.csv")
                    || parser.getFilePath().contains("delta_38206_Admin_UserInRole_20211124002215_93F5B33A-BD08-4B24-97A5-5B809CBF4C9C.csv")
                    || parser.getFilePath().contains("delta_38206_Admin_UserInRole_20211124002215_5AEE4A09-B9A6-411D-A13D-B9E88A9A3DBA.csv")
                    || parser.getFilePath().contains("delta_38206_Admin_UserInRole_20211124002215_B3FA020B-05FC-4196-A4DE-E91B70BEF1D2.csv")
                    || parser.getFilePath().contains("delta_38206_Admin_UserInRole_20211124002215_C28E1D77-5A54-455B-89D5-0BD6E7B23C8C.csv")
                    || parser.getFilePath().contains("delta_38206_Admin_UserInRole_20211124002215_66B86AD2-A450-4FFE-8E3D-64DDAFDF7227.csv")

                    || parser.getFilePath().contains("delta_38211_Admin_UserInRole_20211125002954_2D106103-404D-4054-89B8-18F95932B092.csv")
                    || parser.getFilePath().contains("delta_38211_Admin_UserInRole_20211125002954_93F5B33A-BD08-4B24-97A5-5B809CBF4C9C.csv")
                    || parser.getFilePath().contains("delta_38211_Admin_UserInRole_20211125002954_5AEE4A09-B9A6-411D-A13D-B9E88A9A3DBA.csv")
                    || parser.getFilePath().contains("delta_38211_Admin_UserInRole_20211125002954_B3FA020B-05FC-4196-A4DE-E91B70BEF1D2.csv")
                    || parser.getFilePath().contains("delta_38211_Admin_UserInRole_20211125002954_C28E1D77-5A54-455B-89D5-0BD6E7B23C8C.csv")
                    || parser.getFilePath().contains("delta_38211_Admin_UserInRole_20211125002954_66B86AD2-A450-4FFE-8E3D-64DDAFDF7227.csv")

                    || parser.getFilePath().contains("bulk_38372_Admin_UserInRole_20211206001532_5AEE4A09-B9A6-411D-A13D-B9E88A9A3DBA.csv")
                    || parser.getFilePath().contains("bulk_38372_Admin_UserInRole_20211206001532_C28E1D77-5A54-455B-89D5-0BD6E7B23C8C.csv")

                    // Received on 7th March 2024 following the migration to the v8 schema. These are approx. 1.2gb and contain
                    // a significant amount of irrelevant user in role records which block the transform for every publisher
                    || parser.getFilePath().contains("delta_59833_Admin_UserInRole_20240306000006_2D106103-404D-4054-89B8-18F95932B092")  //EMIS001_AWS
                    || parser.getFilePath().contains("delta_59833_Admin_UserInRole_20240306000006_93F5B33A-BD08-4B24-97A5-5B809CBF4C9C")  //EMIS002_AWS
                    || parser.getFilePath().contains("delta_59833_Admin_UserInRole_20240306000006_B3FA020B-05FC-4196-A4DE-E91B70BEF1D2")  //EMIS004_AWS
                    || parser.getFilePath().contains("delta_59833_Admin_UserInRole_20240306000006_5AEE4A09-B9A6-411D-A13D-B9E88A9A3DBA")  //EMIS003_AWS
                    || parser.getFilePath().contains("delta_59833_Admin_UserInRole_20240306000006_C28E1D77-5A54-455B-89D5-0BD6E7B23C8C")  //EMIS005_AWS
                    || parser.getFilePath().contains("delta_59833_Admin_UserInRole_20240306000006_66B86AD2-A450-4FFE-8E3D-64DDAFDF7227")  //EMIS006_AWS
                    //recieved on 29th april 2025 with 6gb user in role file
                    || parser.getFilePath().contains("bulk_70670_Admin_UserInRole_20250429000003_B3FA020B-05FC-4196-A4DE-E91B70BEF1D2") //EMIS004_AWS

            ) {
                LOG.info("Erroneous UserInRole file: "+parser.getFilePath()+ " skipped for service: "+csvHelper.getServiceId());
                return;
            }

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
