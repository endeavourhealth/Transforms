package org.endeavourhealth.transform.bhrut.transforms;

import org.apache.commons.lang3.ArrayUtils;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.ods.OdsOrganisation;
import org.endeavourhealth.common.ods.OdsWebService;
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

import java.util.HashMap;
import java.util.Map;

public class PMIPreTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(PMIPreTransformer.class);
    private static final String[] V_CODES = {"V81997", "V81998", "V81999"};
    private static final Map<String, String> odsCodesMap;

    static {

        odsCodesMap = new HashMap<String, String>();
        odsCodesMap.put("RF4TC", "North East London NHS Treatment Centre");
        odsCodesMap.put("RF4AD", "Baddow Hospital");
        odsCodesMap.put("RF4BM", "BMI The London Independent Hospital");
        odsCodesMap.put("RF4CF", "The Chelmsford Private Day Surgery Hospital");
        odsCodesMap.put("RF4CP", "Chartwell Private Hospital");
        odsCodesMap.put("RF4HH", "Holly House Hospital");
        odsCodesMap.put("RF4NH", "Nuffield Health Hospital");
        odsCodesMap.put("RF4RH", "Spire London East Hospital");
    }

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
                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser) parser)) {
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


        String orgName = null;

        OrganizationBuilder organizationBuilder
                = csvHelper.getOrgCache().getOrCreateOrganizationBuilder(orgId, csvHelper);

        if (organizationBuilder == null) {
            TransformWarnings.log(LOG, parser, "Error creating Organization resource for ODS: {}", orgId);
            return;
        }

        if (odsCodesMap.containsKey(orgId)) {
            orgName = odsCodesMap.get(orgId);
            organizationBuilder.setName(orgName);
        } else {
            OdsOrganisation org = new OdsOrganisation();
            try {
                org = OdsWebService.lookupOrganisationViaRest(orgId);
            } catch (Exception e) {
                TransformWarnings.log(LOG, parser, "Exception looking up Organization for ODS: {} Exception : {} Line {}", orgId, e.getMessage());
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
        }

        //set the ods identifier
        organizationBuilder.getIdentifiers().clear();
        IdentifierBuilder identifierBuilder = new IdentifierBuilder(organizationBuilder);
        identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
        identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
        identifierBuilder.setValue(orgId);

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), organizationBuilder);

        //add to cache
        csvHelper.getOrgCache().returnOrganizationBuilder(orgId, organizationBuilder);
    }
}
