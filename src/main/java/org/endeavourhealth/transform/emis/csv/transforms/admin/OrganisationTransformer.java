package org.endeavourhealth.transform.emis.csv.transforms.admin;

import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.schema.OrganisationType;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisAdminCacheFiler;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.admin.Organisation;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class OrganisationTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(OrganisationTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        EmisAdminCacheFiler adminCacheFiler = new EmisAdminCacheFiler(csvHelper.getDataSharingAgreementGuid());

        AbstractCsvParser parser = parsers.get(Organisation.class);
        while (parser.nextRecord()) {

            try {
                createResource((Organisation)parser, fhirResourceFiler, csvHelper, adminCacheFiler);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

        adminCacheFiler.close();
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
            identifierBuilder.setSystem(FhirUri.IDENTIFIER_SYSTEM_ODS_CODE);
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
            identifierBuilder.setSystem(FhirUri.IDENTIFIER_SYSTEM_EMIS_CDB_NUMBER);
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
            OrganisationType fhirOrgType = convertOrganisationType(organisationType.getString());
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

        //this resource exists in our admin resource cache, so we can populate the
        //main database when new practices come on, so we need to update that too
        adminCacheFiler.saveAdminResourceToCache(parser.getCurrentState(), organizationBuilder);
    }

    /*private static void createResource(Organisation parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper,
                                       EmisAdminCacheFiler adminCacheFiler) throws Exception {

        Organization fhirOrganisation = new Organization();
        fhirOrganisation.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_ORGANIZATION));

        String orgGuid = parser.getOrganisationGuid();
        fhirOrganisation.setId(orgGuid);

        String odsCode = parser.getODScode();
        Identifier fhirIdentifier = IdentifierHelper.createOdsOrganisationIdentifier(odsCode);
        fhirOrganisation.addIdentifier(fhirIdentifier);

        String name = parser.getOrganisatioName();
        fhirOrganisation.setName(name);

        Date openDate = parser.getOpenDate();
        Date closeDate = parser.getCloseDate();
        Period fhirPeriod = PeriodHelper.createPeriod(openDate, closeDate);
        fhirOrganisation.setActive(PeriodHelper.isActive(fhirPeriod));
        fhirOrganisation.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.ACTIVE_PERIOD, fhirPeriod));

        String parentOrganisationGuid = parser.getParentOrganisationGuid();
        if (!Strings.isNullOrEmpty(parentOrganisationGuid)) {
            fhirOrganisation.setPartOf(csvHelper.createOrganisationReference(parentOrganisationGuid));
        }

        String ccgOrganisationGuid = parser.getCCGOrganisationGuid();
        if (!Strings.isNullOrEmpty(ccgOrganisationGuid)
                && ccgOrganisationGuid.equals(orgGuid)) { //some EMIS CCG orgs seem to refer to themselves, so ignore those
            fhirOrganisation.setPartOf(csvHelper.createOrganisationReference(ccgOrganisationGuid));
        }

        String organisationType = parser.getOrganisationType();
        if (!Strings.isNullOrEmpty(organisationType)) {
            OrganisationType fhirOrgType = convertOrganisationType(organisationType);
            if (fhirOrgType != null) {
                fhirOrganisation.setType(CodeableConceptHelper.createCodeableConcept(fhirOrgType));
            } else {
                //if the org type from the CSV can't be mapped to one of the value set, store as a freetext type
                //LOG.info("Unmapped organisation type " + organisationType);
                fhirOrganisation.setType(CodeableConceptHelper.createCodeableConcept(organisationType));
            }
        }

        String mainLocationGuid = parser.getMainLocationGuid();

        //latest Emis data (31 Jan 2017) has a null record for this value, so handle it being missing
        if (!Strings.isNullOrEmpty(mainLocationGuid)) {
            Reference fhirReference = csvHelper.createLocationReference(mainLocationGuid);
            fhirOrganisation.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.ORGANISATION_MAIN_LOCATION, fhirReference));
        }

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), fhirOrganisation);

        //this resource exists in our admin resource cache, so we can populate the
        //main database when new practices come on, so we need to update that too
        adminCacheFiler.saveAdminResourceToCache(parser.getCurrentState(), fhirOrganisation);
    }*/

    private static OrganisationType convertOrganisationType(String csvOrganisationType) {
        try {
            return OrganisationType.fromDescription(csvOrganisationType);
        } catch (Exception ex) {

            //the below mappings are based on what was present in the EMIS CSV sample files
            //EMIS has been asked for a complete list, but until this is made available, these
            //are the only known types. There are a number of organisation types, such as "Hospice"
            //or "Community" which don't map to any official NHS organisation type
            if (csvOrganisationType.equalsIgnoreCase("General Practice")
                || csvOrganisationType.equalsIgnoreCase("General Practice Surgery")
                || csvOrganisationType.equalsIgnoreCase("Main Surgery")) {
                return OrganisationType.GP_PRACTICE;

            } else if (csvOrganisationType.equalsIgnoreCase("CCG")) {
                return OrganisationType.CCG;

            } else if (csvOrganisationType.equalsIgnoreCase("PCT Site")
                    || csvOrganisationType.equalsIgnoreCase("Primary Care Trust")) {
                return OrganisationType.PCT;

            } else if (csvOrganisationType.equalsIgnoreCase("Hospital")
                    || csvOrganisationType.equalsIgnoreCase("NHS Trust Site")
                    || csvOrganisationType.equalsIgnoreCase("NHS Trust")) {
                return OrganisationType.NHS_TRUST;

            } else {
                return null;
            }
        }

    }
}