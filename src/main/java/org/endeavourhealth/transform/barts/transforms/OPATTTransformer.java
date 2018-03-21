package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.AddressConverter;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.EncounterResourceCache;
import org.endeavourhealth.transform.barts.schema.OPATT;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class OPATTTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(OPATTTransformer.class);
    private static InternalIdDalI internalIdDAL = null;
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

        if (internalIdDAL == null) {
            internalIdDAL = DalProvider.factoryInternalIdDal();
        }

        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell activeCell = parser.getActiveIndicator();
        CsvCell personIdCell = parser.getPatientId();
        CsvCell beginDateCell = parser.getAppointmentDateTime();
        CsvCell apptLengthCell = parser.getExpectedAppointmentDuration();
        CsvCell finIdCell = parser.getFINNo();
        CsvCell outcomeCell = parser.getAttendanceOutcomeCode();

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

        // Delete existing encounter ?
        if (encounterBuilder != null && !activeCell.getIntAsBoolean()) {
            encounterBuilder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);
            //LOG.debug("Delete Encounter (PatId=" + personIdCell.getString() + "):" + FhirSerializationHelper.serializeResource(encounterBuilder.getResource()));
            EncounterResourceCache.deleteEncounterBuilder(encounterBuilder);
            return;
        }

        // Organisation
        Address fhirOrgAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
        ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);

        // Create new encounter
        if (encounterBuilder == null) {
            encounterBuilder = EncounterResourceCache.createEncounterBuilder(encounterIdCell);
        }

        //EpisodOfCare
        EpisodeOfCareBuilder episodeOfCareBuilder = readOrCreateEpisodeOfCareBuilder(null, finIdCell, encounterIdCell, personIdCell, null, csvHelper, fhirResourceFiler, internalIdDAL);
        //LOG.debug("episodeOfCareBuilder:" + episodeOfCareBuilder.getResourceId() + ":" + FhirSerializationHelper.serializeResource(episodeOfCareBuilder.getResource()));

        encounterBuilder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);

        episodeOfCareBuilder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);

        encounterBuilder.setClass(Encounter.EncounterClass.OUTPATIENT);

        // Start date
        encounterBuilder.setPeriodStart(beginDate);

        if (episodeOfCareBuilder.getRegistrationStartDate() == null || episodeOfCareBuilder.getRegistrationStartDate().after(beginDate)) {
            episodeOfCareBuilder.setRegistrationStartDate(beginDate, beginDateCell);
        }

        // End date
        if (endDate != null) {
            encounterBuilder.setPeriodEnd(endDate);

            if (episodeOfCareBuilder.getRegistrationEndDate() == null || episodeOfCareBuilder.getRegistrationEndDate().before(endDate)) {
                episodeOfCareBuilder.setRegistrationEndDate(endDate, apptLengthCell);
            }
        }

        // Status
        if (outcomeCell != null && outcomeCell.getString().trim().length() > 0 && outcomeCell.getInt() == 1) {
            encounterBuilder.setStatus(Encounter.EncounterState.FINISHED, outcomeCell);
            //Status on episodeOfCareBuilder should be set automatically when end-date is set
        }


        if (LOG.isDebugEnabled()) {
            LOG.debug("episodeOfCare Complete:" + FhirSerializationHelper.serializeResource(episodeOfCareBuilder.getResource()));
            LOG.debug("encounter complete:" + FhirSerializationHelper.serializeResource(encounterBuilder.getResource()));
        }


    }
}
