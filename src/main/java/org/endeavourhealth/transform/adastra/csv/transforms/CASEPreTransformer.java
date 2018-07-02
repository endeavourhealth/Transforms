package org.endeavourhealth.transform.adastra.csv.transforms;

import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.adastra.cache.OrganisationResourceCache;
import org.endeavourhealth.transform.adastra.csv.schema.CASE;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
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
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    public static void createResource(CASE parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      AdastraCsvHelper csvHelper,
                                      String version) throws Exception {

        CsvCell caseId = parser.getCaseId();
        CsvCell caseNo = parser.getCaseNo();
        CsvCell patientId = parser.getPatientId();

        // first up, create the OOH organisation
        UUID serviceId = parser.getServiceId();
        LOG.trace("Case Pre-Transformer started for ServiceId: {}", serviceId.toString());
        OrganizationBuilder organizationBuilder
                = OrganisationResourceCache.getOrCreateOrganizationBuilder (serviceId, csvHelper, fhirResourceFiler, parser);
        if (organizationBuilder == null) {
            TransformWarnings.log(LOG, parser, "Error creating Organization resource for ServiceId: {}",
                    serviceId.toString());
            return;
        }

        //simply cache the case Patient and CaseNo references here for use in Consultation, Clinical Code,
        //Prescription and Notes transforms
        if (!caseId.isEmpty()) {

            if (!patientId.isEmpty()) {
                csvHelper.cacheCasePatient(caseId.getString(), patientId);
            }

            if (!caseNo.isEmpty()) {
                csvHelper.cacheCaseCaseNo(caseId.getString(), caseNo);
            }
        } else {
            TransformWarnings.log(LOG, parser, "No Case Id in Case record for PatientId: {},  file: {}",
                    patientId.getString(), parser.getFilePath());
            return;
        }
    }
}
