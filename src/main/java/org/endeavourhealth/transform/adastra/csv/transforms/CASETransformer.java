package org.endeavourhealth.transform.adastra.csv.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.adastra.csv.schema.CASE;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
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

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(CASE parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      AdastraCsvHelper csvHelper,
                                      String version) throws Exception {

        CsvCell patientId = parser.getPatientId();
        CsvCell caseId = parser.getCaseId();

        EpisodeOfCareBuilder episodeBuilder
                = csvHelper.getEpisodeOfCareCache().getOrCreateEpisodeOfCareBuilder(caseId, csvHelper, fhirResourceFiler);

        CsvCell caseNo = parser.getCaseNo();
        if (!caseNo.isEmpty()) {

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(episodeBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.USUAL);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ADASTRA_CASENO);
            identifierBuilder.setValue(caseNo.getString(), caseNo);
        }

        CsvCell caseTag = parser.getCaseTagName();
        if (caseTag != null && !caseTag.isEmpty()) {

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(episodeBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ADASTRA_CASETAG);
            identifierBuilder.setValue(caseTag.getString(), caseTag);
        }

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

        //get the organization resource has been created already in CASEPreTransformer set the episode managing org reference
        UUID serviceId = parser.getServiceId();
        Reference organisationReference = csvHelper.createOrganisationReference(serviceId.toString());

        // if episode already ID mapped, get the mapped ID for the org
        if (isResourceMapped) {

            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, fhirResourceFiler);
        }
        episodeBuilder.setManagingOrganisation(organisationReference);

        //v2 userRef
        CsvCell userRef = parser.getUserRef();
        if (userRef !=null && !userRef.isEmpty()) {

            Reference practitionerReference = csvHelper.createPractitionerReference(userRef.toString());

            if (isResourceMapped) {
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
            }
            episodeBuilder.setCareManager(practitionerReference, userRef);
        }

        //simple priority text set as an extension
        CsvCell priority = parser.getPriorityName();
        if (!priority.isEmpty()) {

            episodeBuilder.setPriority (priority.getString(), priority);
        }

        //v2 pcc arrival date and time
        CsvCell pccArrival = parser.getArrivedPCC();
        if (pccArrival != null && !pccArrival.isEmpty()) {

            episodeBuilder.setPCCArrival(pccArrival.getDateTime(), pccArrival);
        }

        // return the builder back to the cache
        csvHelper.getEpisodeOfCareCache().returnEpisodeOfCareBuilder(caseId, episodeBuilder);
    }
}
