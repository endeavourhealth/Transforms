package org.endeavourhealth.transform.adastra.csv.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.adastra.cache.EpisodeOfCareResourceCache;
import org.endeavourhealth.transform.adastra.cache.OrganisationResourceCache;
import org.endeavourhealth.transform.adastra.csv.schema.CASE;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

public class CASETransformer {

    private static final Logger LOG = LoggerFactory.getLogger(CASETransformer.class);

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

        CsvCell patientId = parser.getPatientId();
        CsvCell caseId = parser.getCaseId();

        EpisodeOfCareBuilder episodeBuilder
                = EpisodeOfCareResourceCache.getOrCreateEpisodeOfCareBuilder(caseId, csvHelper, fhirResourceFiler);

        CsvCell caseNo = parser.getCaseNo();
        IdentifierBuilder identifierBuilder = new IdentifierBuilder(episodeBuilder);
        identifierBuilder.setUse(Identifier.IdentifierUse.USUAL);
        identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ADASTRA_CASENO);
        identifierBuilder.setValue(caseNo.getString(), caseNo);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        boolean isResourceMapped = csvHelper.isResourceIdMapped(caseId.getString(), episodeBuilder.getResource());
        if (isResourceMapped) {
            patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
        }
        episodeBuilder.setPatient(patientReference, patientId);

        CsvCell startDateTime = parser.getStartDateTime();
        if (!startDateTime.isEmpty()) {
            episodeBuilder.setRegistrationStartDate(startDateTime.getDateTime(), startDateTime);
        }

        CsvCell endDateTime = parser.getEndDateTime();
        if (!endDateTime.isEmpty()) {
            episodeBuilder.setRegistrationEndDate(endDateTime.getDateTime(), endDateTime);
        }

        //get the configured serviceId to retrieve an Organization resource if it exists (create if not)
        UUID serviceId = parser.getServiceId();
        OrganizationBuilder organizationBuilder
                = OrganisationResourceCache.getOrCreateOrganizationBuilder (serviceId, csvHelper, fhirResourceFiler, parser);
        //the organization resource has been created or is already created so set the episode managing org reference
        if (organizationBuilder != null) {

            Reference organisationReference = csvHelper.createOrganisationReference(serviceId.toString());
            // if episode already ID mapped, get the mapped ID for the org
            if (isResourceMapped) {
                organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, fhirResourceFiler);
            }
            episodeBuilder.setManagingOrganisation(organisationReference);
        }

        //simple priority text set as an extension
        CsvCell priority = parser.getPriorityName();
        if (!priority.isEmpty()) {
            episodeBuilder.setPriority (priority.getString(), priority);
        }
    }
}
