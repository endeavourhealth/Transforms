package org.endeavourhealth.transform.bhrut.transforms;

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

import java.util.Map;

public class PMIPreTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(PMIPreTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BhrutCsvHelper csvHelper) throws Exception {

        PMI parser = (PMI) parsers.get(PMI.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    if (parser.getRegisteredGpPracticeCode() != null) {
                        CsvCell gpPracticeCodeCell = parser.getRegisteredGpPracticeCode();
                        String gpPracticeCode = gpPracticeCodeCell.getString();
                        createResource(parser, fhirResourceFiler, csvHelper, version, gpPracticeCode);
                    }
                    createResource(parser, fhirResourceFiler, csvHelper, version, BhrutCsvToFhirTransformer.BHRUT_ORG_ODS_CODE);
                } catch (Exception ex) {
                    throw new TransformException(parser.getCurrentState().toString(), ex);
                }
            }
        }
    }


    public static void createResource(PMI parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      BhrutCsvHelper csvHelper,
                                      String version, String codeType) throws Exception {


        boolean orgInCache = csvHelper.getOrgCache().orgCodeInCache(codeType);
        if (!orgInCache) {
            boolean oohOrgAlreadyFiled
                    = csvHelper.getOrgCache().organizationInDB(codeType, csvHelper);
            if (!oohOrgAlreadyFiled) {
                createOrganisation(parser, fhirResourceFiler, csvHelper, codeType);
            }
        }
    }

    public static void createOrganisation(PMI parser, FhirResourceFiler fhirResourceFiler, BhrutCsvHelper csvHelper, String codeType) throws Exception {

        OrganizationBuilder organizationBuilder
                = csvHelper.getOrgCache().getOrCreateOrganizationBuilder(codeType, csvHelper);
        if (organizationBuilder == null) {
            TransformWarnings.log(LOG, parser, "Error creating OOH Organization resource for ODS: {}",
                    codeType);
            return;
        }

        OdsOrganisation org = OdsWebService.lookupOrganisationViaRest(codeType);
        if (org != null) {

            organizationBuilder.setName(org.getOrganisationName());
        } else {

            TransformWarnings.log(LOG, parser, "Error looking up Organization for ODS: {}",
                    codeType);
            return;
        }

        //set the ods identifier
        organizationBuilder.getIdentifiers().clear();
        IdentifierBuilder identifierBuilder = new IdentifierBuilder(organizationBuilder);
        identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
        identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
        identifierBuilder.setValue(codeType);

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), organizationBuilder);

        //add to cache
        csvHelper.getOrgCache().returnOrganizationBuilder(codeType, organizationBuilder);
    }
}
