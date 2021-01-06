package org.endeavourhealth.transform.tpp.csv.helpers.cache;

import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.ScheduleBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.hl7.fhir.instance.model.Schedule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class RotaDetailsCache {
    private static final Logger LOG = LoggerFactory.getLogger(RotaDetailsCache.class);

    private Map<Long, RotaDetailsObject> hmCache = new ConcurrentHashMap<>(); //accessed by multiple threads from Rota pre-transformer
    private ReentrantLock lock = new ReentrantLock();

    private RotaDetailsObject getOrCreateCacheObject(CsvCell rotaIdCell) {

        Long rotaId = rotaIdCell.getLong();

        RotaDetailsObject ret = hmCache.get(rotaId);
        if (ret == null) {
            //if null, lock and check again
            try {
                lock.lock();

                ret = hmCache.get(rotaId);

                //if still null, create the object and add to the cache
                if (ret == null) {
                    ret = new RotaDetailsObject();
                    hmCache.put(rotaId, ret);
                }

            } finally {
                lock.unlock();
            }
        }
        return ret;
    }

    /**
     * we only get rota practitioners from the SRAppointment file, so this fn is used to pre-cache that information
     */
    public void cacheRotaProfile(CsvCell rotaIdCell, CsvCell profileIdCell) throws Exception {

        //add to the cache if the cache has no record for this rota or the new start is earlier
        RotaDetailsObject cache = getOrCreateCacheObject(rotaIdCell);

        cache.setClinicianProfileIdCell(profileIdCell);
    }

    public void cacheExistingRotaDetails(CsvCell rotaIdCell, Date startDate, Long profileId) throws Exception {

        //add to the cache if the cache has no record for this rota or the new start is earlier
        RotaDetailsObject cache = getOrCreateCacheObject(rotaIdCell);

        if (startDate != null) {
            cache.setStartDate(startDate);
        }

        if (profileId != null) {
            cache.setClinicianProfileIdCell(CsvCell.factoryDummyWrapper("" + profileId)); //just create a dummy cell around the value
        }
    }

    /**
     * when a SRRota is first created and doesn't yet have any appointments, the transform will create the FHIR Schedule, but
     * won't have the clinician information to set on it (which is only made available in SRAppointment). When the first appt is booked,
     * we'll finally get that information, but won't necessarily receive an update to SRRota, so this fn makes sure that data gets saved
     */
    public void processRemainingRotaDetails(FhirResourceFiler fhirResourceFiler, TppCsvHelper csvHelper) throws Exception {

        for (Long rotaId: hmCache.keySet()) {
            Schedule schedule = (Schedule)csvHelper.retrieveResource("" + rotaId, ResourceType.Schedule);

            //skip if the rota has been deleted (although this shouldn't be possible if we've still got appts for it)
            if (schedule == null) {
                continue;
            }

            ScheduleBuilder scheduleBuilder = new ScheduleBuilder(schedule);

            RotaDetailsObject details = hmCache.get(rotaId);

            CsvCell profileClinicianCell = details.getClinicianProfileIdCell();
            if (profileClinicianCell != null) {
                Reference clinicianReference = csvHelper.createPractitionerReferenceForProfileId(profileClinicianCell);

                //since the Schedule is already ID mapped, we need to explicitly forwards-map here
                clinicianReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(clinicianReference, csvHelper);

                //TPP appts only support one practitioner at a time, so make sure clear the existing one before adding the latest one
                scheduleBuilder.clearActors();

                scheduleBuilder.addActor(clinicianReference, profileClinicianCell);
            }

            //this isn't strictly necessary, since we're just setting the start date to the same date we
            //pre-cached from the rota pre-transformer, but is here for consistency
            Date startDate = details.getStartDate();
            if (startDate != null) {
                scheduleBuilder.setPlanningHorizonStart(startDate);
            }

            fhirResourceFiler.saveAdminResource(null, false, scheduleBuilder);
        }

        //set cache to null. They should not be needed beyond this point and if anything tries
        //to access them, a Null Pointer Exception will tell us it's happening
        this.hmCache = null;
    }

    public RotaDetailsObject getCachedDetails(CsvCell rotaIdCell) {
        Long rotaId = rotaIdCell.getLong();
        return hmCache.remove(rotaId);
    }
}
