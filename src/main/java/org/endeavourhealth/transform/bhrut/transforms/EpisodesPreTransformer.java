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
import org.endeavourhealth.transform.common.resourceBuilders.NameBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerBuilder;
import org.hl7.fhir.instance.model.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.endeavourhealth.common.ods.OdsWebService.lookupOrganisationViaRest;

public class EpisodesPreTransformer {


    private static final Logger LOG = LoggerFactory.getLogger(EpisodesPreTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BhrutCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Episodes.class);

        if (parser != null) {
            while (parser.nextRecord()) {
                if (!csvHelper.processRecordFilteringOnPatientId(parser)) {
                    continue;
                }
                try {
                    Episodes episodesParser = (Episodes) parser;

                    if (!episodesParser.getAdmissionHospitalCode().isEmpty()) {
                        CsvCell admissionHospitalCodeCell = episodesParser.getAdmissionHospitalCode();
                        String admissionHospitalCode = admissionHospitalCodeCell.getString();
                        createResource(episodesParser, fhirResourceFiler, csvHelper, admissionHospitalCode);
                    }
                    if (!episodesParser.getEpisodeConsultantCode().isEmpty()) {
                        CsvCell episodeConsultantCodeCell = episodesParser.getEpisodeConsultantCode();
                        String episodeConsultantCode = episodeConsultantCodeCell.getString();
                        createEpisodeResource(episodesParser, fhirResourceFiler, csvHelper, episodeConsultantCode);
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
            CsvCell admissionHospitalCode = parser.getAdmissionHospitalCode();
            String code = admissionHospitalCode.getString();
            String name = csvHelper.getOrgCache().getNameForOrgCode(code);
            if (Strings.isNullOrEmpty(name)) {
                OdsOrganisation or = lookupOrganisationViaRest(code);
                if (or != null) { //Ignore non-ODS codes.  Bhrut sometimes uses ePact codes.
                    csvHelper.getOrgCache().addOrgCode(admissionHospitalCode, parser.getAdmissionHospitalName());
                }
            }
        }
    }

    private static void createEpisodeResource(Episodes episodesParser,
                                              FhirResourceFiler fhirResourceFiler,
                                              BhrutCsvHelper csvHelper,
                                              String episodeConsultantCode) throws Exception {

        boolean practitionerCodeInCache = csvHelper.getStaffCache().practitionerCodeInCache(episodeConsultantCode);
        if (!practitionerCodeInCache) {
            boolean practitionerCodeResourceAlreadyFiled
                    = csvHelper.getStaffCache().practitionerCodeInDB(episodeConsultantCode, csvHelper);
            if (!practitionerCodeResourceAlreadyFiled) {
                createPractitionerResource(episodesParser, fhirResourceFiler, csvHelper, episodeConsultantCode);
            }
        }
    }

    public static void createResource(Episodes parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      BhrutCsvHelper csvHelper,
                                      String orgId) throws Exception {

        boolean orgInCache = csvHelper.getOrgCache().organizationInCache(orgId);
        if (!orgInCache) {
            boolean orgResourceAlreadyFiled
                    = csvHelper.getOrgCache().organizationInDB(orgId, csvHelper);
            if (!orgResourceAlreadyFiled) {
                createOrganisation(parser, fhirResourceFiler, csvHelper, orgId);
            }
        }
    }

    private static void createPractitionerResource(Episodes episodesParser,
                                                   FhirResourceFiler fhirResourceFiler,
                                                   BhrutCsvHelper csvHelper,
                                                   String episodeConsultantCode) throws Exception {

        PractitionerBuilder practitionerBuilder
                = csvHelper.getStaffCache().getOrCreatePractitionerBuilder(episodeConsultantCode, csvHelper);

        CsvCell episodeConsultantCodeCell = episodesParser.getEpisodeConsultantCode();
        if (!episodeConsultantCodeCell.isEmpty()) {
            IdentifierBuilder.removeExistingIdentifiersForSystem(practitionerBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_CONSULTANT_CODE);
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CONSULTANT_CODE);
            identifierBuilder.setValue(episodeConsultantCodeCell.getString(), episodeConsultantCodeCell);
        }

        CsvCell episodeConsultant = episodesParser.getEpisodeConsultant();
        NameBuilder nameBuilder = new NameBuilder(practitionerBuilder);
        nameBuilder.setText(episodeConsultant.getString(), episodeConsultant);
        fhirResourceFiler.saveAdminResource(episodesParser.getCurrentState(), practitionerBuilder);
        csvHelper.getStaffCache().cachePractitionerBuilder(episodeConsultantCode, practitionerBuilder);
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

//            TransformWarnings.log(LOG, parser, "Error looking up Organization for ODS: {}", orgId);
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
        csvHelper.getOrgCache().cacheOrganizationBuilder(orgId, organizationBuilder);
    }
}
