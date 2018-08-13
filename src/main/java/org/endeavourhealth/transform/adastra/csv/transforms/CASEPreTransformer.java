package org.endeavourhealth.transform.adastra.csv.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
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

        // first up, create the OOH organisation from the DDS service details
        UUID serviceId = parser.getServiceId();
        boolean orgInCache = csvHelper.getOrganisationCache().organizationInCache(serviceId.toString());

        // if this is the first run, the organization will not have been created or cached - will only run once for th OOH org
        if (!orgInCache) {

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

            //save the new OOH organization resource
            //boolean mapIds = !organizationBuilder.isIdMapped();
            //fhirResourceFiler.saveAdminResource(parser.getCurrentState(), mapIds, organizationBuilder);
            fhirResourceFiler.saveAdminResource(parser.getCurrentState(), organizationBuilder);

            //add to cache
            csvHelper.getOrganisationCache().returnOrganizationBuilder(serviceId.toString(), organizationBuilder);
        }

        // next up, simply cache the case Patient and CaseNo references here for use in Consultation, Clinical Code,
        // Prescription and Notes transforms
        CsvCell caseId = parser.getCaseId();
        CsvCell caseNo = parser.getCaseNo();
        CsvCell patientId = parser.getPatientId();

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
