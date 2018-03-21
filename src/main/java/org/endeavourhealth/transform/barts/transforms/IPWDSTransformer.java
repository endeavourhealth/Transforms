package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.PeriodHelper;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.EncounterResourceCache;
import org.endeavourhealth.transform.barts.cache.LocationResourceCache;
import org.endeavourhealth.transform.barts.schema.IPWDS;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
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
                                 List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    createEpisodeEventWardStay((IPWDS) parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
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
        if (encounterBuilder == null) {
            if (activeCell.getIntAsBoolean()) {
                TransformWarnings.log(LOG, parser, "Skipping encounter ward stay {} because Encounter unknown in file {}", encounterIdCell.getString(), parser.getFilePath());
            }
            return;
        }

        // Patient
        UUID patientUuid = csvHelper.findPatientIdFromPersonId(personIdCell);
        if (patientUuid == null) {
            TransformWarnings.log(LOG, parser, "Skipping encounter {} because no Person->MRN mapping {} could be found in file {}", encounterIdCell.getString(), personIdCell.getString(), parser.getFilePath());
            return;
        }

        // Delete existing encounter ? - Assuming this will be signaled in ENCNT
        /*
        if (encounterBuilder != null && !activeCell.getIntAsBoolean()) {
            encounterBuilder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);
            //LOG.debug("Delete Encounter (PatId=" + personIdCell.getString() + "):" + FhirSerializationHelper.serializeResource(encounterBuilder.getResource()));
            EncounterResourceCache.deleteEncounterBuilder(encounterBuilder);
            return;
        }*/

        // Organisation
        //Address fhirOrgAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
        //ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);

        // Location
        UUID locationResourceUUID = null;

        Encounter.EncounterLocationComponent elc = new Encounter.EncounterLocationComponent();
        elc.setPeriod(wardStayPeriod);
        elc.setStatus(getLocationStatus(wardStayPeriod));

        // Use bed location ?
        if (bedLocationIdCell != null && !bedLocationIdCell.isEmpty() && bedLocationIdCell.getLong() > 0) {
            locationResourceUUID = LocationResourceCache.getOrCreateLocationUUID(csvHelper, bedLocationIdCell);
            elc.setLocation(ReferenceHelper.createReference(ResourceType.Location, locationResourceUUID.toString()));
            // Remove duplicate (if exists)
            List<Encounter.EncounterLocationComponent> locationList = encounterBuilder.getLocation();
            if (locationList != null) {
                for (Encounter.EncounterLocationComponent currELC : locationList) {
                    if (currELC.getLocation().getReference().compareToIgnoreCase(elc.getLocation().getReference()) == 0) {
                        if (currELC.hasPeriod() && currELC.getPeriod().getStart().compareTo(elc.getPeriod().getStart()) == 0) {
                            locationList.remove(currELC);
                        }
                    }
                }
            }
            // Add location
            encounterBuilder.addLocation(ReferenceHelper.createReference(ResourceType.Location, locationResourceUUID.toString()), bedLocationIdCell, beginDateCell, endDateCell);
        } else if (roomLocationIdCell != null && !roomLocationIdCell.isEmpty() && roomLocationIdCell.getLong() > 0) {
            locationResourceUUID = LocationResourceCache.getOrCreateLocationUUID(csvHelper, roomLocationIdCell);
            elc.setLocation(ReferenceHelper.createReference(ResourceType.Location, locationResourceUUID.toString()));
            // Remove duplicate (if exists)
            List<Encounter.EncounterLocationComponent> locationList = encounterBuilder.getLocation();
            if (locationList != null) {
                for (Encounter.EncounterLocationComponent currELC : locationList) {
                    if (currELC.getLocation().getReference().compareToIgnoreCase(elc.getLocation().getReference()) == 0) {
                        if (currELC.hasPeriod() && currELC.getPeriod().getStart().compareTo(elc.getPeriod().getStart()) == 0) {
                            locationList.remove(currELC);
                        }
                    }
                }
            }
            // Add location
            encounterBuilder.addLocation(ReferenceHelper.createReference(ResourceType.Location, locationResourceUUID.toString()), roomLocationIdCell, beginDateCell, endDateCell);
        } else if (locationIdCell != null && !locationIdCell.isEmpty() && locationIdCell.getLong() > 0) {
            locationResourceUUID = LocationResourceCache.getOrCreateLocationUUID(csvHelper, locationIdCell);
            elc.setLocation(ReferenceHelper.createReference(ResourceType.Location, locationResourceUUID.toString()));
            // Remove duplicate (if exists)
            List<Encounter.EncounterLocationComponent> locationList = encounterBuilder.getLocation();
            if (locationList != null) {
                for (Encounter.EncounterLocationComponent currELC : locationList) {
                    if (currELC.getLocation().getReference().compareToIgnoreCase(elc.getLocation().getReference()) == 0) {
                        if (currELC.hasPeriod() && currELC.getPeriod().getStart().compareTo(elc.getPeriod().getStart()) == 0) {
                            locationList.remove(currELC);
                        }
                    }
                }
            }
            // Add location
            encounterBuilder.addLocation(ReferenceHelper.createReference(ResourceType.Location, locationResourceUUID.toString()), locationIdCell, beginDateCell, endDateCell);
        } else {
            TransformWarnings.log(LOG, parser, "Location Resource not found for Location-id {} in IPWDS record {} in file {}", locationIdCell.getString(), encounterIdCell.getString(), parser.getFilePath());
        }



        if (LOG.isDebugEnabled()) {
            //LOG.debug("episodeOfCare Complete:" + FhirSerializationHelper.serializeResource(episodeOfCareBuilder.getResource()));
            LOG.debug("encounter complete:" + FhirSerializationHelper.serializeResource(encounterBuilder.getResource()));
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
