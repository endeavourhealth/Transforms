package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.AddressHelper;
import org.endeavourhealth.common.fhir.PeriodHelper;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.EncounterResourceCache;
import org.endeavourhealth.transform.barts.cache.LocationResourceCache;
import org.endeavourhealth.transform.barts.schema.OPATT;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class OPATTTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(OPATTTransformer.class);
    private static SimpleDateFormat formatDaily = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    private static SimpleDateFormat formatBulk = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");


    public static void transform(String version,
                                 List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    createOutpatientAttendanceEvent((OPATT) parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    public static void createOutpatientAttendanceEvent(OPATT parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell activeCell = parser.getActiveIndicator();
        CsvCell personIdCell = parser.getPatientId();
        CsvCell beginDateCell = parser.getAppointmentDateTime();
        CsvCell apptLengthCell = parser.getExpectedAppointmentDuration();
        CsvCell finIdCell = parser.getFINNo();
        CsvCell outcomeCell = parser.getAttendanceOutcomeCode();
        CsvCell currentLocationCell = parser.getLocationCode();

        if (!activeCell.getIntAsBoolean()) {
            // skip - inactive entries contains no useful data
            return;
        }

        Date beginDate = null;
        if (beginDateCell != null && !beginDateCell.isEmpty()) {
            try {
                beginDate = formatDaily.parse(beginDateCell.getString());
            } catch (ParseException ex) {
                beginDate = formatBulk.parse(beginDateCell.getString());
            }
        }
        Date endDate = null;
        if (beginDate != null) {
            if (apptLengthCell != null && !apptLengthCell.isEmpty() && apptLengthCell.getInt() > 0) {
                endDate = new Date(beginDate.getTime() + (apptLengthCell.getInt() * 60 * 1000));
            } else {
                endDate = beginDate;
            }
        }

        // get the associated encounter
        EncounterBuilder encounterBuilder = EncounterResourceCache.getEncounterBuilder(csvHelper, encounterIdCell.getString());
        if (encounterBuilder == null && !activeCell.getIntAsBoolean()) {
            // skip - encounter missing but set to delete so do nothing
            return;
        }

        // Patient
        UUID patientUuid = csvHelper.findPatientIdFromPersonId(personIdCell);
        if (patientUuid == null) {
            TransformWarnings.log(LOG, parser, "Skipping encounter {} because no Person->MRN mapping {} could be found in file {}", encounterIdCell.getString(), personIdCell.getString(), parser.getFilePath());
            return;
        }

        // Organisation
        Address fhirOrgAddress = AddressHelper.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
        ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);

        //EpisodOfCare
        EpisodeOfCareBuilder episodeOfCareBuilder = readOrCreateEpisodeOfCareBuilder(null, finIdCell, encounterIdCell, personIdCell, patientUuid, csvHelper, parser);
        LOG.debug("episodeOfCareBuilder:" + FhirSerializationHelper.serializeResource(episodeOfCareBuilder.getResource()));
        if (encounterBuilder != null && episodeOfCareBuilder.getResourceId().compareToIgnoreCase(ReferenceHelper.getReferenceId(encounterBuilder.getEpisodeOfCare().get(0))) != 0) {
            LOG.debug("episodeOfCare reference has changed from " + encounterBuilder.getEpisodeOfCare().get(0).getReference() + " to " + episodeOfCareBuilder.getResourceId());
        }

        // Create new encounter
        if (encounterBuilder == null) {
            encounterBuilder = EncounterResourceCache.createEncounterBuilder(encounterIdCell, finIdCell);

            encounterBuilder.addEpisodeOfCare(ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeOfCareBuilder.getResourceId()), finIdCell);
        }


        episodeOfCareBuilder.setManagingOrganisation((ReferenceHelper.createReference(ResourceType.Organization, organisationResourceId.getResourceId().toString())));

        encounterBuilder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);

        episodeOfCareBuilder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);

        encounterBuilder.setClass(Encounter.EncounterClass.OUTPATIENT);

        if (beginDate != null) {
            // Start date
            encounterBuilder.setPeriodStart(beginDate);

            if (episodeOfCareBuilder.getRegistrationStartDate() == null || beginDate.before(episodeOfCareBuilder.getRegistrationStartDate())) {
                episodeOfCareBuilder.setRegistrationStartDate(beginDate, beginDateCell);
                episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.ACTIVE);
            }

            // End date
            if (endDate != null) {
                encounterBuilder.setPeriodEnd(endDate);

                encounterBuilder.setStatus(Encounter.EncounterState.FINISHED, outcomeCell);

                if (episodeOfCareBuilder.getRegistrationEndDate() == null || endDate.after(episodeOfCareBuilder.getRegistrationEndDate())) {
                    episodeOfCareBuilder.setRegistrationEndDateNoStatusUpdate(endDate, apptLengthCell);
                }

            } else if (beginDate.before(new Date())) {
                encounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS, outcomeCell);
            } else {
                encounterBuilder.setStatus(Encounter.EncounterState.PLANNED, outcomeCell);
            }
        } else {
            encounterBuilder.setStatus(Encounter.EncounterState.PLANNED);
            if (episodeOfCareBuilder.getRegistrationEndDate() == null) {
                episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.PLANNED);
            }
        }

        // Check whether to Finish EpisodeOfCare
        // EoC Status (for some reason it can contain just spaces)
        // Outcome = 1 means discharged from care
        if (outcomeCell != null && outcomeCell.getString().trim().length() > 0 && outcomeCell.getInt() == 1) {
            episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.FINISHED, outcomeCell);
        }

        // Location
        if (currentLocationCell != null && !currentLocationCell.isEmpty() && currentLocationCell.getLong() > 0) {
            UUID locationResourceUUID = LocationResourceCache.getOrCreateLocationUUID(csvHelper, currentLocationCell);
            if (locationResourceUUID != null) {
                if (beginDate == null || endDate == null) {
                    encounterBuilder.addLocation(ReferenceHelper.createReference(ResourceType.Location, locationResourceUUID.toString()), true, currentLocationCell);
                } else {
                    Period apptPeriod = PeriodHelper.createPeriod(beginDate, endDate);
                    Encounter.EncounterLocationComponent elc = new Encounter.EncounterLocationComponent();
                    elc.setPeriod(apptPeriod);
                    elc.setLocation(ReferenceHelper.createReference(ResourceType.Location, locationResourceUUID.toString()));
                    encounterBuilder.addLocation(ReferenceHelper.createReference(ResourceType.Location, locationResourceUUID.toString()), true, currentLocationCell, beginDateCell, apptLengthCell);
                }
            } else {
                TransformWarnings.log(LOG, parser, "Location Resource not found for Location-id {} in OPATT record {} in file {}", currentLocationCell.getString(), encounterIdCell.getString(), parser.getFilePath());
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("episodeOfCare Complete:" + FhirSerializationHelper.serializeResource(episodeOfCareBuilder.getResource()));
            LOG.debug("encounter complete:" + FhirSerializationHelper.serializeResource(encounterBuilder.getResource()));
        }


    }
}
