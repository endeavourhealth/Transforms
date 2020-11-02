package org.endeavourhealth.transform.bhrut.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.ods.OdsOrganisation;
import org.endeavourhealth.common.ods.OdsWebService;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.schema.Spells;
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

//import static org.hl7.fhir.instance.model.ResourceType.Encounter;

public class SpellsPreTransformer {


    private static final Logger LOG = LoggerFactory.getLogger(SpellsPreTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BhrutCsvHelper csvHelper) throws Exception {

        Spells parser = (Spells) parsers.get(Spells.class);

        if (parser != null) {
            while (parser.nextRecord()) {
                if (!csvHelper.processRecordFilteringOnPatientId(parser)) {
                    continue;
                }
                try {

                    CsvCell admissionHospitalCodeCell = parser.getAdmissionHospitalCode();
                    if (!admissionHospitalCodeCell.isEmpty()) {

                        String admissionHospitalCode = admissionHospitalCodeCell.getString();
                        createOrganisationResource(parser, fhirResourceFiler, csvHelper, admissionHospitalCode);
                    }

                    createConsultantResources(parser, fhirResourceFiler, csvHelper);

                    if (!parser.getDataUpdateStatus().getString().equalsIgnoreCase("Deleted")) {
                        cacheResources(parser, fhirResourceFiler, csvHelper);
                    }

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }


    public static void cacheResources(Spells parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      BhrutCsvHelper csvHelper) throws Exception {

        if (!parser.getAdmissionConsultantCode().isEmpty()) {
            String name = csvHelper.getStaffCache().getNameForCcode(parser.getAdmissionConsultantCode().getString());
            if (Strings.isNullOrEmpty(name)) {
                csvHelper.getStaffCache().addConsultantCode(parser.getAdmissionConsultantCode(), parser.getAdmissionConsultant());
            }
        }
        if (!parser.getDischargeConsultantCode().isEmpty()) {
            String name = csvHelper.getStaffCache().getNameForCcode(parser.getDischargeConsultantCode().getString());
            if (Strings.isNullOrEmpty(name)) {
                csvHelper.getStaffCache().addConsultantCode(parser.getDischargeConsultantCode(), parser.getDischargeConsultant());
            }
        }

        if (!parser.getAdmissionHospitalCode().isEmpty()) {
            String name = csvHelper.getOrgCache().getNameForOrgCode(parser.getAdmissionHospitalCode().getString());
            if (Strings.isNullOrEmpty(name)) {
                csvHelper.getOrgCache().addOrgCode(parser.getAdmissionHospitalCode(), parser.getAdmissionHospitalName());
            }
        }
        if (!parser.getDischargeHospitalCode().isEmpty()) {
            String name = csvHelper.getOrgCache().getNameForOrgCode(parser.getDischargeHospitalCode().getString());
            if (Strings.isNullOrEmpty(name)) {
                csvHelper.getOrgCache().addOrgCode(parser.getDischargeHospitalCode(), parser.getDischargeHospitalName());
            }
        }
        if (!parser.getPasId().isEmpty()) {
            String gpCode = csvHelper.getPasIdtoGPCache().getGpCodeforPasId(parser.getPasId().getString());
            if (Strings.isNullOrEmpty(gpCode)) {
                csvHelper.getPasIdtoGPCache().addGpCode(parser.getPasId(), parser.getSpellRegisteredGpPracticeCode());
            }
        }
    }

    private static void createConsultantResources(Spells spellsParser, FhirResourceFiler fhirResourceFiler, BhrutCsvHelper csvHelper) throws Exception {

        //the admission consultant
        CsvCell admissionConsultantCodeCell = spellsParser.getAdmissionConsultantCode();
        if (!admissionConsultantCodeCell.isEmpty()) {

            String admissionConsultantCode = admissionConsultantCodeCell.getString();
            boolean admissionConsultantCodeInCache = csvHelper.getStaffCache().practitionerCodeInCache(admissionConsultantCode);
            if (!admissionConsultantCodeInCache) {

                boolean admissionConsultantCodeResourceAlreadyFiled
                        = csvHelper.getStaffCache().practitionerCodeInDB(admissionConsultantCode, csvHelper);
                if (!admissionConsultantCodeResourceAlreadyFiled) {

                    CsvCell consultantNameCell = spellsParser.getAdmissionConsultant();
                    createPractitionerResource(spellsParser,
                            fhirResourceFiler,
                            csvHelper,
                            admissionConsultantCodeCell,
                            consultantNameCell);
                }
            }
        }

        //the discharge consultant
        CsvCell dischargeConsultantCodeCell = spellsParser.getDischargeConsultantCode();
        if (!dischargeConsultantCodeCell.isEmpty()) {

            String dischargeConsultantCode = dischargeConsultantCodeCell.getString();
            boolean dischargeConsultantCodeInCache = csvHelper.getStaffCache().practitionerCodeInCache(dischargeConsultantCode);
            if (!dischargeConsultantCodeInCache) {

                boolean dischargeConsultantCodeResourceAlreadyFiled
                        = csvHelper.getStaffCache().practitionerCodeInDB(dischargeConsultantCode, csvHelper);
                if (!dischargeConsultantCodeResourceAlreadyFiled) {

                    CsvCell consultantNameCell = spellsParser.getDischargeConsultant();
                    createPractitionerResource(spellsParser,
                            fhirResourceFiler,
                            csvHelper,
                            dischargeConsultantCodeCell,
                            consultantNameCell);
                }
            }
        }
    }

    private static void createPractitionerResource(Spells spellsParser,
                                                   FhirResourceFiler fhirResourceFiler,
                                                   BhrutCsvHelper csvHelper,
                                                   CsvCell consultantCodeCell,
                                                   CsvCell consultantNameCell) throws Exception {

        PractitionerBuilder practitionerBuilder
                = csvHelper.getStaffCache().getOrCreatePractitionerBuilder(consultantCodeCell.getString(), csvHelper);

        IdentifierBuilder.removeExistingIdentifiersForSystem(practitionerBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_CONSULTANT_CODE);
        IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
        identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CONSULTANT_CODE);
        identifierBuilder.setValue(consultantCodeCell.getString(), consultantCodeCell);

        NameBuilder nameBuilder = new NameBuilder(practitionerBuilder);
        nameBuilder.setText(consultantNameCell.getString(), consultantNameCell);
        fhirResourceFiler.saveAdminResource(spellsParser.getCurrentState(), practitionerBuilder);

        csvHelper.getStaffCache().cachePractitionerBuilder(consultantCodeCell.getString(), practitionerBuilder);
    }

    public static void createOrganisationResource(Spells parser,
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

    public static void createOrganisation(Spells parser,
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

           // TransformWarnings.log(LOG, parser, "Error looking up Organization for ODS: {}", orgId);
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
