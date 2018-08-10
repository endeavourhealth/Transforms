package org.endeavourhealth.transform.adastra.csv.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.adastra.csv.schema.PROVIDER;
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

public class PROVIDERTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(PROVIDERTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 AdastraCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(PROVIDER.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((PROVIDER) parser, fhirResourceFiler, csvHelper, version);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(PROVIDER parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      AdastraCsvHelper csvHelper,
                                      String version) throws Exception {

        CsvCell patientIdCell = parser.getPatientId();
        CsvCell gpPracticeCodeCell = parser.getGPPracticeNatCode();
        if (gpPracticeCodeCell.isEmpty()) {
            TransformWarnings.log(LOG, parser, "Provider GP Practice Code is blank for PatientId: {}",
                    patientIdCell.getString());
            return;
        }

        String gpPracticeCode = gpPracticeCodeCell.getString();
        OrganizationBuilder organizationBuilder
                = csvHelper.getOrganisationCache().getOrCreateOrganizationBuilder(  gpPracticeCode,
                                                                                    csvHelper,
                                                                                    fhirResourceFiler,
                                                                                    parser);
        if (organizationBuilder == null) {
            TransformWarnings.log(LOG, parser,
                    "Error creating or retrieving Provider Organization resource for Practice Code: {}",
                                gpPracticeCode);
            return;
        }

        // if this is the first run, the organization will not have been created or cached, so do this
        if (!csvHelper.getOrganisationCache().organizationInCache(gpPracticeCode)) {

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(organizationBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
            identifierBuilder.setValue(gpPracticeCode);

            CsvCell gpPracticeName = parser.getGPPracticeName();
            organizationBuilder.setName(gpPracticeName.getString());

            CsvCell gpPracticePostCodeCell = parser.getGPPracticePostcode();
            organizationBuilder.addAddress().setPostalCode(gpPracticePostCodeCell.getString());

            //save the new OOH organization resource
            fhirResourceFiler.saveAdminResource(parser.getCurrentState(), organizationBuilder);

            //add to cache
            csvHelper.getOrganisationCache().returnOrganizationBuilder(gpPracticeCode, organizationBuilder);
        }

        //cache the patient and practice code map to retrieve in the Patient transform
        csvHelper.cachePatientCareProvider(patientIdCell.getString(), gpPracticeCodeCell);
    }
}
