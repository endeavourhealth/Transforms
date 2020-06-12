package org.endeavourhealth.transform.tpp.csv.helpers.cache;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.PeriodHelper;
import org.endeavourhealth.common.fhir.schema.RegistrationStatus;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.hl7.fhir.instance.model.*;

import java.util.*;

public class TppRecordStatusCache {

    private Map<Long, List<MedicalRecordStatusCacheObject>> medicalRecordStatusMap = new HashMap<>(); //patient ID is the key

    public static boolean addRecordStatuses(List<MedicalRecordStatusCacheObject> statuses, EpisodeOfCareBuilder episodeBuilder) throws Exception {

        if (statuses == null
                || statuses.isEmpty()) {
            return false;
        }

        ContainedListBuilder containedListBuilder = new ContainedListBuilder(episodeBuilder);

        //use a set for combining the existing and new stauts records, so duplicates automatically get removed
        Set<RecordStatusWrapper> fullSet = new HashSet<>();

        //find any existing statuses on the episode
        List<List_.ListEntryComponent> items = containedListBuilder.getContainedListItems();
        if (items != null) {
            for (List_.ListEntryComponent item : items) {
                Date entryDate = item.getDate();
                CodeableConcept codeableConcept = item.getFlag();
                Coding coding = CodeableConceptHelper.getFirstCoding(codeableConcept);
                String code = coding.getCode();
                RegistrationStatus status = RegistrationStatus.fromCode(code);

                fullSet.add(new RecordStatusWrapper(entryDate, status));
            }
        }

        //add new/updates statuses
        for (MedicalRecordStatusCacheObject status : statuses) {
            fullSet.add(new RecordStatusWrapper(status));
        }

        //sort
        List<RecordStatusWrapper> fullList = new ArrayList<>(fullSet);
        fullList.sort(((o1, o2) -> {
            if (o1.getDate() == null && o2.getDate() == null) {
                return 0;
            } else if (o1.getDate() == null) {
                return -1;
            } else if (o2.getDate() == null) {
                return 1;
            } else {
                return o1.getDate().compareTo(o2.getDate());
            }
        }));

        //remove and add
        containedListBuilder.removeContainedList();

        for (RecordStatusWrapper wrapper : fullList) {

            CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(wrapper.getRegistrationStatus());
            containedListBuilder.addCodeableConcept(codeableConcept, wrapper.getRegistrationStatusCell());
            if (wrapper.getDate() != null) {
                containedListBuilder.addDateToLastItem(wrapper.getDate(), wrapper.getDateCell());
            }
        }

        return true;
    }


    public static RegistrationStatus convertMedicalRecordStatus(CsvCell statusCell) throws Exception {
        int medicalRecordStatus = statusCell.getInt().intValue();
        switch (medicalRecordStatus) {
            case 0:
                return RegistrationStatus.DEDUCTED_RECORDS_SENT_BACK_TO_FHSA;
            case 1:
                return RegistrationStatus.REGISTERED_RECORD_SENT_FROM_FHSA;
            case 2:
                return RegistrationStatus.REGISTERED_RECORD_RECEIVED_FROM_FHSA;
            case 3:
                return RegistrationStatus.DEDUCTED_RECORDS_RECEIVED_BY_FHSA;
            case 4:
                return RegistrationStatus.DEDUCTED_RECORD_REQUESTED_BY_FHSA;
            default:
                throw new TransformException("Unmapped medical record status " + medicalRecordStatus);
        }
    }


    public void cacheMedicalRecordStatus(CsvCell patientGuid, CsvCell dateCell, CsvCell medicalRecordStatusCell) throws Exception {

        //ignore if we don't have one of the key elements
        if (dateCell.isEmpty()
                || medicalRecordStatusCell.isEmpty()) {
            return;
        }


        Long key = patientGuid.getLong();
        List<MedicalRecordStatusCacheObject> list = medicalRecordStatusMap.get(key);
        if (list == null) {
            list = new ArrayList<>();
            medicalRecordStatusMap.put(key, list);
        }

        list.add(new MedicalRecordStatusCacheObject(dateCell, medicalRecordStatusCell));
    }

    public List<MedicalRecordStatusCacheObject> getMedicalRecordStatusesForEpisode(CsvCell patientGuid, EpisodeOfCareBuilder episodeOfCareBuilder) throws Exception {
        return getMedicalRecordStatusesForEpisode(patientGuid.getLong(), episodeOfCareBuilder);
    }

    public List<MedicalRecordStatusCacheObject> getMedicalRecordStatusesForEpisode(Long patientId, EpisodeOfCareBuilder episodeOfCareBuilder) throws Exception {

        List<MedicalRecordStatusCacheObject> list = medicalRecordStatusMap.get(patientId);
        if (list == null) {
            return new ArrayList<>();
        }

        //filter the status records by date, so we only find the ones within the date period
        List<MedicalRecordStatusCacheObject> ret = new ArrayList<>();

        EpisodeOfCare episodeOfCare = (EpisodeOfCare)episodeOfCareBuilder.getResource();
        Period activePeriod = episodeOfCare.getPeriod();

        for (int i=list.size()-1; i>=0; i--) {
            MedicalRecordStatusCacheObject status = list.get(i);
            Date statusDate = status.getDateCell().getDateTime();
            if (PeriodHelper.isWithin(activePeriod, statusDate)) {
                list.remove(i);
                ret.add(status);
            }
        }

        return ret;
    }

    public void processRemainingRegistrationStatuses(FhirResourceFiler fhirResourceFiler) throws Exception {

        //make sure all EpisodeOfCare resources done before us are in the DB
        fhirResourceFiler.waitUntilEverythingIsSaved();

        for (Long patientId : medicalRecordStatusMap.keySet()) {

            //we leave the empty list in the map after processing, so just check we have something before retrieving any resources
            List<MedicalRecordStatusCacheObject> remainingStatuses = medicalRecordStatusMap.get(patientId);
            if (remainingStatuses.isEmpty()) {
                continue;
            }

            //map to patient UUID
            UUID patientUuid = IdHelper.getEdsResourceId(fhirResourceFiler.getServiceId(), ResourceType.Patient, "" + patientId);

            //find all episodes of care for the patient
            ResourceDalI resourceDal = DalProvider.factoryResourceDal();
            List<ResourceWrapper> episodeWrappers = resourceDal.getResourcesByPatient(fhirResourceFiler.getServiceId(), patientUuid, ResourceType.EpisodeOfCare.toString());

            for (ResourceWrapper wrapper: episodeWrappers) {
                EpisodeOfCare episodeOfCare = (EpisodeOfCare)wrapper.getResource();
                EpisodeOfCareBuilder episodeBuilder = new EpisodeOfCareBuilder(episodeOfCare);

                List<MedicalRecordStatusCacheObject> statusesForEpisode = getMedicalRecordStatusesForEpisode(patientId, episodeBuilder);
                if (addRecordStatuses(statusesForEpisode, episodeBuilder)) {
                    fhirResourceFiler.savePatientResource(null, false, episodeBuilder);
                }
            }
        }
    }

    static class RecordStatusWrapper {
        private MedicalRecordStatusCacheObject cacheObject;
        private Date date;
        private RegistrationStatus registrationStatus;

        public RecordStatusWrapper(MedicalRecordStatusCacheObject cacheObject) throws Exception {
            this.cacheObject = cacheObject;
            this.date = cacheObject.getDateCell().getDate();
            this.registrationStatus = convertMedicalRecordStatus(cacheObject.getStatusCell());
        }

        public RecordStatusWrapper(Date date, RegistrationStatus registrationStatus) {
            this.date = date;
            this.registrationStatus = registrationStatus;
        }

        public RegistrationStatus getRegistrationStatus() {
            return registrationStatus;
        }

        public Date getDate() {
            return date;
        }

        public CsvCell getRegistrationStatusCell() {
            if (cacheObject != null) {
                return cacheObject.getStatusCell();
            } else {
                return null;
            }
        }

        public CsvCell getDateCell() {
            if (cacheObject != null) {
                return cacheObject.getDateCell();
            } else {
                return null;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            RecordStatusWrapper that = (RecordStatusWrapper) o;

            if (date != null ? !date.equals(that.date) : that.date != null) return false;
            return registrationStatus == that.registrationStatus;

        }

        @Override
        public int hashCode() {
            int result = date != null ? date.hashCode() : 0;
            result = 31 * result + registrationStatus.hashCode();
            return result;
        }
    }
}


