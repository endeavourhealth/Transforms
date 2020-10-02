package org.endeavourhealth.transform.bhrut.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.ods.OdsOrganisation;
import org.endeavourhealth.common.ods.OdsWebService;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.cache.StaffCache;
import org.endeavourhealth.transform.bhrut.schema.Outpatients;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.NameBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerBuilder;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class OutpatientsPreTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(OutpatientsPreTransformer.class);


    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BhrutCsvHelper csvHelper) throws Exception {

        Outpatients parser = (Outpatients) parsers.get(Outpatients.class);

        if (parser != null) {
            while (parser.nextRecord()) {
                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                    continue;
                }
                try {
                    createResource(parser, fhirResourceFiler, csvHelper, version);

                } catch (Exception ex) {
                    throw new TransformException(parser.getCurrentState().toString(), ex);
                }
            }
        }
    }

    public static void createResource(Outpatients parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      BhrutCsvHelper csvHelper,
                                      String version) throws Exception {


        //for each unique ods code in the outpatient file, check, file and cache the resource
        CsvCell hospitalCodeCell = parser.getHospitalCode();
        CsvCell consultantCell = parser.getConsultant();
        CsvCell consultantCodeCell = parser.getConsultantCode();

        if (!hospitalCodeCell.isEmpty()) {
            boolean orgInCache = csvHelper.getOrgCache().organizationInCache(hospitalCodeCell.getString());
            if (!orgInCache) {
                boolean orgResourceAlreadyFiled
                        = csvHelper.getOrgCache().organizationInDB(hospitalCodeCell.getString(), csvHelper);
                if (!orgResourceAlreadyFiled) {
                    createOrganisation(parser, fhirResourceFiler, csvHelper);
                }
            }
        }
        if (!consultantCodeCell.isEmpty()) {
            boolean staffInCache = csvHelper.getStaffCache().cCodeInCache(consultantCodeCell.getString());
            if (!staffInCache) {
                csvHelper.getStaffCache().addConsultantCode(consultantCodeCell, consultantCell);
                PractitionerBuilder practitionerBuilder
                        = csvHelper.getStaffCache().getOrCreatePractitionerBuilder(consultantCodeCell.getString(), csvHelper);
                if (practitionerBuilder.getNames().isEmpty()) {
                    NameBuilder nameBuilder = new NameBuilder(practitionerBuilder);
                    //nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
                    nameBuilder.setText(consultantCell.getString(), consultantCell);
                }
                csvHelper.getStaffCache().returnPractitionerBuilder(practitionerBuilder.getResourceId(), practitionerBuilder);
                fhirResourceFiler.saveAdminResource(parser.getCurrentState(), !practitionerBuilder.isIdMapped(),practitionerBuilder);
            }
        }
    }

    public static void createOrganisation(Outpatients parser, FhirResourceFiler fhirResourceFiler, BhrutCsvHelper csvHelper) throws Exception {

        CsvCell odsCodeCell = parser.getHospitalCode();
        OrganizationBuilder organizationBuilder
                = csvHelper.getOrgCache().getOrCreateOrganizationBuilder(odsCodeCell.getString(), csvHelper);
        if (organizationBuilder == null) {
            TransformWarnings.log(LOG, parser, "Error creating Organization resource for ODS: {}",
                    odsCodeCell.getString());
            return;
        }

        OdsOrganisation org = OdsWebService.lookupOrganisationViaRest(odsCodeCell.getString());
        if (org != null) {

            organizationBuilder.setName(org.getOrganisationName());
        } else {

            TransformWarnings.log(LOG, parser, "Error looking up Organization for ODS: {}",
                    odsCodeCell.getString());
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
        csvHelper.getOrgCache().returnOrganizationBuilder(odsCodeCell.getString(), organizationBuilder);
    }
}
