package org.endeavourhealth.transform.adastra.csv.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.ods.OdsOrganisation;
import org.endeavourhealth.common.ods.OdsWebService;
import org.endeavourhealth.core.database.dal.admin.models.Service;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.adastra.csv.schema.CASE;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.hl7.fhir.instance.model.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

public class CASEPreTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(CASEPreTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 AdastraCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(CASE.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((CASE) parser, fhirResourceFiler, csvHelper, version);
                } catch (Exception ex) {
                    throw new TransformException(parser.getCurrentState().toString(), ex);
                }
            }
        }
    }

    public static void createResource(CASE parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      AdastraCsvHelper csvHelper,
                                      String version) throws Exception {

        //create / cache the OOH code if it exists (v2)
        CsvCell odsCode = parser.getODSCode();
        if (odsCode != null) {

            boolean orgInCache = csvHelper.getOrganisationCache().organizationInCache(odsCode.toString());
            if (!orgInCache) {
                boolean oohOrgAlreadyFiled
                        = csvHelper.getOrganisationCache().organizationInDB(odsCode.toString(), csvHelper, fhirResourceFiler);
                if (!oohOrgAlreadyFiled) {

                    createOOHOrganisation(parser, fhirResourceFiler, csvHelper);
                }
            }
        } else {
            // or, create the OOH organisation from the DDS service details if ods code does not exist
            // if this is the first run, the organization will not have been created yet or cached - will only run once for the OOH service org
            UUID serviceId = parser.getServiceId();
            boolean orgInCache = csvHelper.getOrganisationCache().organizationInCache(serviceId.toString());
            if (!orgInCache) {
                boolean oohServiceOrgAlreadyFiled
                        = csvHelper.getOrganisationCache().organizationInDB(serviceId.toString(), csvHelper, fhirResourceFiler);
                if (!oohServiceOrgAlreadyFiled) {

                    createServiceOOHOrganisation(parser, fhirResourceFiler, csvHelper);
                }
            }
        }

        // next up, simply cache the case Patient, CaseNo and StartDate references here for use in Consultation, Clinical Code,
        // Prescription, Notes and Case Questions transforms
        CsvCell caseId = parser.getCaseId();
        CsvCell caseNo = parser.getCaseNo();
        CsvCell patientId = parser.getPatientId();
        CsvCell caseStartDate = parser.getStartDateTime();

        if (!caseId.isEmpty()) {

            if (!patientId.isEmpty()) {
                csvHelper.cacheCasePatient(caseId.getString(), patientId);
            }

            if (!caseNo.isEmpty()) {
                csvHelper.cacheCaseCaseNo(caseId.getString(), caseNo);
            }

            if (!caseStartDate.isEmpty()) {
                csvHelper.cacheCaseStartDate(caseId.getString(), caseStartDate);
            }

        } else {
            TransformWarnings.log(LOG, parser, "No Case Id in Case record for PatientId: {},  file: {}",
                    patientId.getString(), parser.getFilePath());
            return;
        }
    }

    private static void createServiceOOHOrganisation(CASE parser, FhirResourceFiler fhirResourceFiler, AdastraCsvHelper csvHelper) throws Exception {

        UUID serviceId = parser.getServiceId();

        OrganizationBuilder organizationBuilder
                = csvHelper.getOrganisationCache().getOrCreateOrganizationBuilder (serviceId.toString(), csvHelper, fhirResourceFiler, parser);
        if (organizationBuilder == null) {
            TransformWarnings.log(LOG, parser, "Error creating OOH Organization resource for ServiceId: {}",
                    serviceId.toString());
            return;
        }

        //lookup the Service details from DDS
        Service service = csvHelper.getService(serviceId);
        if (service != null) {

            String localId = service.getLocalId();
            if (!localId.isEmpty()) {
                organizationBuilder.getIdentifiers().clear();
                IdentifierBuilder identifierBuilder = new IdentifierBuilder(organizationBuilder);
                identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
                identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
                identifierBuilder.setValue(localId);
            }

            String serviceName = service.getName();
            if (!serviceName.isEmpty()) {
                organizationBuilder.setName(serviceName);
            }
        }

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), organizationBuilder);

        //add to cache
        csvHelper.getOrganisationCache().returnOrganizationBuilder(serviceId.toString(), organizationBuilder);
    }

    private static void createOOHOrganisation(CASE parser, FhirResourceFiler fhirResourceFiler, AdastraCsvHelper csvHelper) throws Exception {

        CsvCell odsCodeCell = parser.getODSCode();

        OrganizationBuilder organizationBuilder
                = csvHelper.getOrganisationCache().getOrCreateOrganizationBuilder (odsCodeCell.toString(), csvHelper, fhirResourceFiler, parser);
        if (organizationBuilder == null) {
            TransformWarnings.log(LOG, parser, "Error creating OOH Organization resource for ODS: {}",
                    odsCodeCell.toString());
            return;
        }

        OdsOrganisation org = OdsWebService.lookupOrganisationViaRest(odsCodeCell.getString().trim());
        if (org != null) {

            organizationBuilder.setName(org.getOrganisationName());
        } else {

            TransformWarnings.log(LOG, parser, "Error looking up Organization for ODS: {}",
                    odsCodeCell.toString());
            return;
        }

        //set the ods identifier
        organizationBuilder.getIdentifiers().clear();
        IdentifierBuilder identifierBuilder = new IdentifierBuilder(organizationBuilder);
        identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
        identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
        identifierBuilder.setValue(odsCodeCell.getString());

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), organizationBuilder);

        //add to cache
        csvHelper.getOrganisationCache().returnOrganizationBuilder(odsCodeCell.toString(), organizationBuilder);
    }
}
