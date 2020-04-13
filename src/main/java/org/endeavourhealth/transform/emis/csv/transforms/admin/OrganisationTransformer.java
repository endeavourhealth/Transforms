package org.endeavourhealth.transform.emis.csv.transforms.admin;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.schema.OrganisationType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.EmisLocationDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.EmisOrganisationDalI;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisMappingHelper;
import org.endeavourhealth.transform.emis.csv.schema.admin.Organisation;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

public class OrganisationTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(OrganisationTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        Organisation parser = (Organisation)parsers.get(Organisation.class);
        if (parser != null) {

            while (parser.nextRecord()) {
                CsvCell organisationGuid = parser.getOrganisationGuid();
                csvHelper.getAdminHelper().addOrganisationChanged(organisationGuid);
            }

            //the above will have audited the table, so now we can load the bulk staging table with our file
            String filePath = parser.getFilePath();
            Date dataDate = fhirResourceFiler.getDataDate();
            EmisOrganisationDalI dal = DalProvider.factoryEmisOrganisationDal();
            int fileId = parser.getFileAuditId().intValue();
            dal.updateStagingTable(filePath, dataDate, fileId);

            //call this to abort if we had any errors, during the above processing
            fhirResourceFiler.failIfAnyErrors();
        }
    }

    /*private static void createResource(Organisation parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper) throws Exception {

        OrganizationBuilder organizationBuilder = new OrganizationBuilder();

        CsvCell orgGuid = parser.getOrganisationGuid();
        organizationBuilder.setId(orgGuid.getString(), orgGuid);

        CsvCell odsCode = parser.getODScode();
        if (!odsCode.isEmpty()) {
            organizationBuilder.setOdsCode(odsCode.getString(), odsCode);
        }

        CsvCell name = parser.getOrganisatioName();
        if (!name.isEmpty()) {
            organizationBuilder.setName(name.getString(), name);
        }

        CsvCell cdbNumber = parser.getCDB();
        if (!cdbNumber.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(organizationBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_EMIS_CDB_NUMBER);
            identifierBuilder.setValue(cdbNumber.getString(), cdbNumber);
        }

        CsvCell openDate = parser.getOpenDate();
        if (!openDate.isEmpty()) {
            organizationBuilder.setOpenDate(openDate.getDate(), openDate);
        }

        CsvCell closeDate = parser.getCloseDate();
        if (!closeDate.isEmpty()) {
            organizationBuilder.setCloseDate(openDate.getDate(), openDate);
        }

        //FHIR orgs can only support being "part of" one other organisation, so if we have a parent
        //CCG, then use that as the parent, otherwise use the regular parent column
        CsvCell ccgOrganisationGuid = parser.getCCGOrganisationGuid();
        if (!ccgOrganisationGuid.isEmpty()
                && !ccgOrganisationGuid.equalsValue(orgGuid)) { //some EMIS CCG orgs seem to refer to themselves, so ignore those
            Reference parentOrgReference = csvHelper.createOrganisationReference(ccgOrganisationGuid);
            organizationBuilder.setParentOrganisation(parentOrgReference, ccgOrganisationGuid);

        } else {

            CsvCell parentOrganisationGuid = parser.getParentOrganisationGuid();
            if (!parentOrganisationGuid.isEmpty()) {
                Reference parentOrgReference = csvHelper.createOrganisationReference(parentOrganisationGuid);
                organizationBuilder.setParentOrganisation(parentOrgReference, parentOrganisationGuid);
            }
        }

        CsvCell organisationType = parser.getOrganisationType();
        if (!organisationType.isEmpty()) {
            //OrganisationType fhirOrgType = convertOrganisationType(organisationType.getString());
            OrganisationType fhirOrgType = EmisMappingHelper.findOrganisationType(organisationType.getString());
            if (fhirOrgType != null) {
                organizationBuilder.setType(fhirOrgType, organisationType);

            } else {
                //if the org type from the CSV can't be mapped to one of the value set, store as a freetext type
                organizationBuilder.setTypeFreeText(organisationType.getString(), organisationType);
            }
        }

        CsvCell mainLocationGuid = parser.getMainLocationGuid();
        if (!mainLocationGuid.isEmpty()) {
            Reference fhirReference = csvHelper.createLocationReference(mainLocationGuid);
            organizationBuilder.setMainLocation(fhirReference, mainLocationGuid);
        }

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), organizationBuilder);
    }*/

}