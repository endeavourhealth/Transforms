package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.PeriodHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.EncounterResourceCache;
import org.endeavourhealth.transform.barts.schema.IPWDS;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Period;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class IPWDSTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(IPWDSTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    createEpisodeEventWardStay((IPWDS)parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    public static void createEpisodeEventWardStay(IPWDS parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell personIdCell = parser.getPatientId();
        CsvCell activeCell = parser.getActiveIndicator();
        CsvCell wardLocationIdCell = parser.getWardStayLocationCode();
        CsvCell roomLocationIdCell = parser.getWardRoomCode();
        CsvCell bedLocationIdCell = parser.getWardBedCode();
        CsvCell beginDateCell = parser.getWardStayStartDateTime();
        CsvCell endDateCell = parser.getWardStayEndDateTime();

        Date beginDate = null;
        if (!BartsCsvHelper.isEmptyOrIsStartOfTime(beginDateCell)) {
            beginDate = BartsCsvHelper.parseDate(beginDateCell);
        }

        Date endDate = null;
        if (!BartsCsvHelper.isEmptyOrIsEndOfTime(endDateCell)) {
            endDate = BartsCsvHelper.parseDate(endDateCell);
        }

        Period wardStayPeriod = PeriodHelper.createPeriod(beginDate, endDate);

        // get the associated encounter
        EncounterBuilder encounterBuilder = EncounterResourceCache.getEncounterBuilder(encounterIdCell, personIdCell, activeCell, csvHelper);

        //unlike other files, there doesn't seem to be a unique key that we can use to prevent duplicates,
        //so simply find any existing location on the encounter with the same start date and remove it
        //this is because we get multiple rows in IPWDS that all seem to be exactly the same
        CsvCell wardStayIdCell = parser.getCDSWardStayId();
        EncounterBuilder.removeExistingLocation(encounterBuilder, wardStayIdCell.getString());

        if (!activeCell.getIntAsBoolean()) {
            //if inactive, we've already removed the location from the Encounter, so just return out
            return;
        }

        Encounter.EncounterLocationComponent elc = new Encounter.EncounterLocationComponent();
        elc.setId(wardStayIdCell.getString());
        elc.setPeriod(wardStayPeriod);
        elc.setStatus(getLocationStatus(wardStayPeriod));

        if (!BartsCsvHelper.isEmptyOrIsZero(bedLocationIdCell)) {
            Reference locationReference = csvHelper.createLocationReference(bedLocationIdCell);
            if (encounterBuilder.isIdMapped()) {
                locationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(locationReference, fhirResourceFiler);
            }
            elc.setLocation(locationReference);

            encounterBuilder.addLocation(elc, bedLocationIdCell, beginDateCell, endDateCell);

        } else if (!BartsCsvHelper.isEmptyOrIsZero(roomLocationIdCell)) {
            Reference locationReference = csvHelper.createLocationReference(roomLocationIdCell);
            if (encounterBuilder.isIdMapped()) {
                locationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(locationReference, fhirResourceFiler);
            }
            elc.setLocation(locationReference);

            encounterBuilder.addLocation(elc, roomLocationIdCell, beginDateCell, endDateCell);

        } else if (!BartsCsvHelper.isEmptyOrIsZero(wardLocationIdCell )) {
            Reference locationReference = csvHelper.createLocationReference(wardLocationIdCell);
            if (encounterBuilder.isIdMapped()) {
                locationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(locationReference, fhirResourceFiler);
            }
            elc.setLocation(locationReference);

            encounterBuilder.addLocation(elc, wardLocationIdCell, beginDateCell, endDateCell);

        } else {
            TransformWarnings.log(LOG, parser, "Location Resource not found for Location-id {} in IPWDS record {} in file {}", wardLocationIdCell.getString(), encounterIdCell.getString(), parser.getFilePath());
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
