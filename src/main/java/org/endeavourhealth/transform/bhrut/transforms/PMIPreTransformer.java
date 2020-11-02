package org.endeavourhealth.transform.bhrut.transforms;

import org.apache.commons.lang3.ArrayUtils;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.ods.OdsOrganisation;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.BhrutCsvToFhirTransformer;
import org.endeavourhealth.transform.bhrut.schema.PMI;
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

import static org.endeavourhealth.common.ods.OdsWebService.lookupOrganisationViaRest;
import static org.endeavourhealth.transform.bhrut.BhrutCsvHelper.V_CODES;

public class PMIPreTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(PMIPreTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BhrutCsvHelper csvHelper) throws Exception {

        PMI parser = (PMI) parsers.get(PMI.class);
        if (parser != null) {

            //create the top level organisation for BHRUT using the ODS code as a one off
            createResource(parser, fhirResourceFiler, csvHelper, version, BhrutCsvToFhirTransformer.BHRUT_ORG_ODS_CODE);
            long count = 0;
            long checkpoint = 5000;
            while (parser.nextRecord()) {
                if (!csvHelper.processRecordFilteringOnPatientId(parser)) {
                    continue;
                }
                count++;
                try {
                    if (!parser.getRegisteredGpPracticeCode().isEmpty()) {
                        CsvCell gpPracticeCodeCell = parser.getRegisteredGpPracticeCode();
                        String gpPracticeCode = gpPracticeCodeCell.getString();
                        createResource(parser, fhirResourceFiler, csvHelper, version, gpPracticeCode);
                    }
                } catch (Exception ex) {
                    throw new TransformException(parser.getCurrentState().toString(), ex);
                }
                if (count % checkpoint == 0) {
                    LOG.info("PMI processed " + count + " records.");
                }
            }
        }
    }


    public static void createResource(PMI parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      BhrutCsvHelper csvHelper,
                                      String version, String orgId) throws Exception {

        boolean orgInCache = csvHelper.getOrgCache().organizationInCache(orgId);
        if (!orgInCache) {

            boolean orgResourceAlreadyFiled
                    = csvHelper.getOrgCache().organizationInDB(orgId, csvHelper);
            if (!orgResourceAlreadyFiled) {
                createOrganisation(parser, fhirResourceFiler, csvHelper, orgId);
            }
        }
    }

    public static void createOrganisation(PMI parser,
                                          FhirResourceFiler fhirResourceFiler,
                                          BhrutCsvHelper csvHelper,
                                          String orgId) throws Exception {

        OdsOrganisation org = lookupOrganisationViaRest(orgId);
        if (org == null) { //Non ODS code meaning it's probably a misuses ePact code so use Barts RF4 ODS code
            orgId = BhrutCsvToFhirTransformer.BHRUT_ORG_ODS_CODE;
            org = lookupOrganisationViaRest(orgId);
        }

        OrganizationBuilder organizationBuilder
                = csvHelper.getOrgCache().getOrCreateOrganizationBuilder(orgId, csvHelper);

        if (organizationBuilder == null) {
            TransformWarnings.log(LOG, parser, "Error creating Organization resource for ODS: {}", orgId);
            return;
        }

        if (org != null) {
            organizationBuilder.setName(org.getOrganisationName());
        } else {
            if (!ArrayUtils.contains(V_CODES, orgId)) {
                TransformWarnings.log(LOG, parser, "Error looking up Organization for ODS: {} ID  {}", orgId, parser.getId().getString());
            }
            return;

        }
        //set the ods identifier
        organizationBuilder.getIdentifiers().clear();
        IdentifierBuilder identifierBuilder = new IdentifierBuilder(organizationBuilder);
        identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
        identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
        identifierBuilder.setValue(orgId);
        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), !organizationBuilder.isIdMapped() ,organizationBuilder);
        //add to cache
        csvHelper.getOrgCache().cacheOrganizationBuilder(orgId, organizationBuilder);
    }
}
