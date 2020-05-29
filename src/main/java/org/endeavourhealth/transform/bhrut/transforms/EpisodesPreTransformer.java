package org.endeavourhealth.transform.bhrut.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.ods.OdsOrganisation;
import org.endeavourhealth.common.ods.OdsWebService;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.schema.Episodes;
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

public class EpisodesPreTransformer {


    private static final Logger LOG = LoggerFactory.getLogger(EpisodesPreTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BhrutCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Episodes.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    Episodes episodesParser = (Episodes) parser;

                    if (!episodesParser.getAdmissionHospitalCode().isEmpty()) {
                        CsvCell admissionHospitalCodeCell = episodesParser.getAdmissionHospitalCode();
                        String admissionHospitalCode = admissionHospitalCodeCell.getString();
                        createResource(episodesParser, fhirResourceFiler, csvHelper, version, admissionHospitalCode);
                    }

                    if (!episodesParser.getDataUpdateStatus().getString().equalsIgnoreCase("Deleted")) {
                        cacheResources(episodesParser, fhirResourceFiler, csvHelper, version);
                    }
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }


    public static void cacheResources(Episodes parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      BhrutCsvHelper csvHelper,
                                      String version) throws Exception {

        if (!parser.getEpisodeConsultantCode().isEmpty()) {
            String name = csvHelper.getStaffCache().getNameForCcode(parser.getEpisodeConsultantCode().getString());
            if (Strings.isNullOrEmpty(name)) {
                csvHelper.getStaffCache().addConsultantCode(parser.getEpisodeConsultantCode(), parser.getEpisodeConsultant());
            }
        }

        if (!parser.getAdmissionHospitalCode().isEmpty()) {
            String name = csvHelper.getOrgCache().getNameForOrgCode(parser.getAdmissionHospitalCode().getString());
            if (Strings.isNullOrEmpty(name)) {
                csvHelper.getOrgCache().addOrgCode(parser.getAdmissionHospitalCode(), parser.getAdmissionHospitalName());
            }
        }
    }

    public static void createResource(Episodes parser,
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

    public static void createOrganisation(Episodes parser,
                                          FhirResourceFiler fhirResourceFiler,
                                          BhrutCsvHelper csvHelper,
                                          String orgId) throws Exception {

        OrganizationBuilder organizationBuilder
                = csvHelper.getOrgCache().getOrCreateOrganizationBuilder(orgId, csvHelper);
        if (organizationBuilder == null) {
            TransformWarnings.log(LOG, parser, "Error creating Organization resource for ODS: {}", orgId);
            return;
        }

        OdsOrganisation org = OdsWebService.lookupOrganisationViaRest(orgId);
        if (org != null) {

            organizationBuilder.setName(org.getOrganisationName());
        } else {

            TransformWarnings.log(LOG, parser, "Error looking up Organization for ODS: {}", orgId);
            return;
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
