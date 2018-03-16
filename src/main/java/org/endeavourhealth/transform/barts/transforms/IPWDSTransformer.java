package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.AddressConverter;
import org.endeavourhealth.common.fhir.PeriodHelper;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.EncounterResourceCache;
import org.endeavourhealth.transform.barts.schema.IPWDS;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Period;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class IPWDSTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(IPWDSTransformer.class);
    private static InternalIdDalI internalIdDAL = null;
    private static SimpleDateFormat formatDaily = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    private static SimpleDateFormat formatBulk = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");


    public static void transform(String version,
                                 ParserI parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        if (parser == null) {
            return;
        }

        while (parser.nextRecord()) {
            try {
                createEpisodeEventWardStay((IPWDS)parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    /*
     *
     */
    public static void createEpisodeEventWardStay(IPWDS parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        if (internalIdDAL == null) {
            internalIdDAL = DalProvider.factoryInternalIdDal();
        }

        EpisodeOfCareBuilder episodeOfCareBuilder = null;
        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell personIdCell = parser.getPatientId();
        CsvCell activeCell = parser.getActiveIndicator();
        CsvCell locationIdCell = parser.getWardStayLocationCode();
        CsvCell roomLocationIdCell = parser.getWardRoomCode();
        CsvCell bedLocationIdCell = parser.getWardBedCode();
        CsvCell beginDateCell = parser.getWardStayStartDateTime();
        CsvCell endDateCell = parser.getWardStayEndDateTime();
        CsvCell sequenceNumberCell = parser.getWardStaySequenceNumber();

        Date beginDate = null;
        if (beginDateCell != null && !beginDateCell.isEmpty()) {
            try {
                beginDate = formatDaily.parse(beginDateCell.getString());
            } catch (ParseException ex) {
                beginDate = formatBulk.parse(beginDateCell.getString());
            }
        }
        Date endDate = null;
        if (endDateCell != null && !endDateCell.isEmpty()) {
            try {
                endDate = formatDaily.parse(endDateCell.getString());
            } catch (ParseException ex) {
                endDate = formatBulk.parse(endDateCell.getString());
            }
        }
        Period wardStayPeriod = PeriodHelper.createPeriod(beginDate, endDate);

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

        encounterBuilder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);

        // Location
        UUID locationResourceUUID = null;

        Encounter.EncounterLocationComponent elc = new Encounter.EncounterLocationComponent();
        elc.setPeriod(wardStayPeriod);
        elc.setStatus(getLocationStatus(wardStayPeriod));

        List<Encounter.EncounterLocationComponent> locationList = encounterBuilder.getLocation();
        if (locationList != null && sequenceNumberCell.getInt() <= locationList.size()) {
            // Update existing location
            locationList.remove(sequenceNumberCell.getInt() - 1);
        }

        // Use bed location ?
        if (bedLocationIdCell != null && !bedLocationIdCell.isEmpty() && bedLocationIdCell.getLong() > 0) {
            locationResourceUUID = csvHelper.lookupLocationUUID(bedLocationIdCell.getString(), fhirResourceFiler, parser);
        }
        if (locationResourceUUID != null) {
            elc.setLocation(ReferenceHelper.createReference(ResourceType.Location, locationResourceUUID.toString()));
            encounterBuilder.addLocation(ReferenceHelper.createReference(ResourceType.Location, locationResourceUUID.toString()), bedLocationIdCell, beginDateCell, endDateCell);
        } else {
            TransformWarnings.log(LOG, parser, "Location Resource not found for Location-id {} in IPWDS record {} in file {}", bedLocationIdCell.getString(), encounterIdCell.getString(), parser.getFilePath());
        }

        // Use room location ?
        if (locationResourceUUID == null) {
            if (roomLocationIdCell != null && !roomLocationIdCell.isEmpty() && roomLocationIdCell.getLong() > 0) {
                locationResourceUUID = csvHelper.lookupLocationUUID(roomLocationIdCell.getString(), fhirResourceFiler, parser);
            }
            if (locationResourceUUID != null) {
                elc.setLocation(ReferenceHelper.createReference(ResourceType.Location, locationResourceUUID.toString()));
                encounterBuilder.addLocation(ReferenceHelper.createReference(ResourceType.Location, locationResourceUUID.toString()), roomLocationIdCell, beginDateCell, endDateCell);
            } else {
                TransformWarnings.log(LOG, parser, "Location Resource not found for Location-id {} in IPWDS record {} in file {}", roomLocationIdCell.getString(), encounterIdCell.getString(), parser.getFilePath());
            }
        }

        // Use location ?
        if (locationResourceUUID == null) {
            if (locationIdCell != null && !locationIdCell.isEmpty() && locationIdCell.getLong() > 0) {
                locationResourceUUID = csvHelper.lookupLocationUUID(locationIdCell.getString(), fhirResourceFiler, parser);
            }
            if (locationResourceUUID != null) {
                elc.setLocation(ReferenceHelper.createReference(ResourceType.Location, locationResourceUUID.toString()));
                encounterBuilder.addLocation(ReferenceHelper.createReference(ResourceType.Location, locationResourceUUID.toString()), locationIdCell, beginDateCell, endDateCell);
            } else {
                TransformWarnings.log(LOG, parser, "Location Resource not found for Location-id {} in IPWDS record {} in file {}", locationIdCell.getString(), encounterIdCell.getString(), parser.getFilePath());
            }
        }

    }

    private static Encounter.EncounterLocationStatus getLocationStatus(Period p) {
        Date date = new Date();
        if (p.getStart() == null || p.getStart().after(date)) {
            return Encounter.EncounterLocationStatus.PLANNED;
        } else if (p.getEnd() != null) {
            return Encounter.EncounterLocationStatus.COMPLETED;
        } else {
            return Encounter.EncounterLocationStatus.ACTIVE;
        }
    }


}
