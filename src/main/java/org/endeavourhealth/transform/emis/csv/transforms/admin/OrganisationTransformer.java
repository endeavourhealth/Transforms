package org.endeavourhealth.transform.emis.csv.transforms.admin;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.schema.OrganisationType;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisAdminCacheFiler;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisMappingHelper;
import org.endeavourhealth.transform.emis.csv.schema.admin.Organisation;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class OrganisationTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(OrganisationTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        EmisAdminCacheFiler adminCacheFiler = new EmisAdminCacheFiler(csvHelper.getDataSharingAgreementGuid());

        AbstractCsvParser parser = parsers.get(Organisation.class);
        while (parser != null && parser.nextRecord()) {

            try {
                createResource((Organisation)parser, fhirResourceFiler, csvHelper, adminCacheFiler);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

        adminCacheFiler.close();

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createResource(Organisation parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper,
                                       EmisAdminCacheFiler adminCacheFiler) throws Exception {

        OrganizationBuilder organizationBuilder = new OrganizationBuilder();

        CsvCell orgGuid = parser.getOrganisationGuid();
        organizationBuilder.setId(orgGuid.getString(), orgGuid);

        CsvCell odsCode = parser.getODScode();
        if (!odsCode.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(organizationBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
            identifierBuilder.setValue(odsCode.getString(), odsCode);
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

        //this resource exists in our admin resource cache, so we can populate the
        //main database when new practices come on, so we need to update that too
        adminCacheFiler.saveAdminResourceToCache(organizationBuilder);

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), organizationBuilder);
    }


    /*public static OrganisationType convertOrganisationType(String csvOrganisationType) {
        try {
            return OrganisationType.fromDescription(csvOrganisationType);
        } catch (Exception ex) {

            //the below mappings are based on what was present in the EMIS CSV sample files
            //EMIS has been asked for a complete list, but until this is made available, these
            //are the only known types. There are a number of organisation types, such as "Hospice"
            //or "Community" which don't map to any official NHS organisation type
            if (csvOrganisationType.equalsIgnoreCase("General Practice")
                || csvOrganisationType.equalsIgnoreCase("General Practice Surgery")
                || csvOrganisationType.equalsIgnoreCase("Main Surgery")
                || csvOrganisationType.equalsIgnoreCase("GP Contract")
                || csvOrganisationType.equalsIgnoreCase("GP Practice")
                || csvOrganisationType.equalsIgnoreCase("GP Surgery")
                || csvOrganisationType.equalsIgnoreCase("G.P.Surgery")
            ) {
                return OrganisationType.GP_PRACTICE;

            } else if (csvOrganisationType.equalsIgnoreCase("CCG")) {
                return OrganisationType.CCG;

            } else if (csvOrganisationType.equalsIgnoreCase("PCT Site")
                    || csvOrganisationType.equalsIgnoreCase("Primary Care Trust")
                    || csvOrganisationType.equalsIgnoreCase("PCT")
            ) {
                return OrganisationType.PCT;

            } else if (csvOrganisationType.equalsIgnoreCase("NHS Trust Site")
                    || csvOrganisationType.equalsIgnoreCase("NHS Trust")) {
                return OrganisationType.NHS_TRUST;

            } else if (csvOrganisationType.equalsIgnoreCase("Community")
                    || csvOrganisationType.equalsIgnoreCase("Community Unit")
                    || csvOrganisationType.equalsIgnoreCase("Community Health Clinic")
                    || csvOrganisationType.equalsIgnoreCase("Community Clinic")) {
                return OrganisationType.COMMUNITY;

            } else if (csvOrganisationType.equalsIgnoreCase("Hospice")) {
                return OrganisationType.HOSPICE;

            } else if (csvOrganisationType.equalsIgnoreCase("Hospital")
                    || csvOrganisationType.equalsIgnoreCase("Hospitals")
                    || csvOrganisationType.equalsIgnoreCase("Hospitals Trust")) {
                return OrganisationType.HOSPITAL;

            } else if (csvOrganisationType.equalsIgnoreCase("Clinic")
                    || csvOrganisationType.equalsIgnoreCase("Centre")
                    || csvOrganisationType.equalsIgnoreCase("Surgery or Clinic")
                    || csvOrganisationType.equalsIgnoreCase("Health Centres")
                    || csvOrganisationType.equalsIgnoreCase("Clinic NHS")) {
                return OrganisationType.CLINIC;

            } else if (csvOrganisationType.equalsIgnoreCase("Strategic Health Authority")) {
                return OrganisationType.STRATEGIC_HEALTH_AUTHORITY;

            } else if (csvOrganisationType.equalsIgnoreCase("Health Board")) {
                return OrganisationType.WELSH_LOCAL_HEALTH_BOARD;

            } else if (csvOrganisationType.equalsIgnoreCase("Health Authority")
                    || csvOrganisationType.equalsIgnoreCase("Health Authority (trading)")) {
                return OrganisationType.HEALTH_AUTHORITY;

            } else if (csvOrganisationType.equalsIgnoreCase("Primary Care Centre")
                    || csvOrganisationType.equalsIgnoreCase("Primary Care Organisation")) {
                return OrganisationType.PRIMARY_CARE_ORGANISATION;

            } else if (csvOrganisationType.equalsIgnoreCase("Branch Surgery")) {
                return OrganisationType.GP_BRANCH_SURGERY;

            } else if (csvOrganisationType.equalsIgnoreCase("Clinic Private")
                    || csvOrganisationType.equalsIgnoreCase("Private Clinic")) {
                return OrganisationType.PRIVATE_CLINIC;

            } else if (csvOrganisationType.equalsIgnoreCase("Hospital Department")) {
                return OrganisationType.HOSPITAL_DEPARTMENT;

            } else if (csvOrganisationType.equalsIgnoreCase("Out of Hours, Non Practice")) {
                return OrganisationType.OUT_OF_HOURS;

            } else if (csvOrganisationType.equalsIgnoreCase("Community Mental Health team")) {
                return OrganisationType.COMMUNITY_MENTAL_HEALTH_TEAM;

            } else if (csvOrganisationType.equalsIgnoreCase("Nursing Home")) {
                return OrganisationType.NURSING_HOME;

            } else {
                return null;
            }
        }

    }*/
}