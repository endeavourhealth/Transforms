package org.endeavourhealth.transform.tpp.csv.helpers.cache;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.schema.RegistrationStatus;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppConfigListOption;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ReferralRequestBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.hl7.fhir.instance.model.*;

import java.util.*;

public class TppReferralStatusCache {

    private Map<Long, List<ReferralStatusRecord>> referralStatusRecords = new HashMap<>(); //keyed against referral ID

    /**
     * called after the SRReferralOutTransformer to deal with any new/updated status records that
     * didn't also have a SRReferralOut in the same exchange
     */
    public void processRemainingReferralStatuses(FhirResourceFiler fhirResourceFiler) throws Exception {

        //make sure all ReferralRequest resources done before us are in the DB
        fhirResourceFiler.waitUntilEverythingIsSaved();

        for (Long referralId: referralStatusRecords.keySet()) {
            List<ReferralStatusRecord> statuses = referralStatusRecords.get(referralId);

            //map to patient UUID
            UUID referralUuid = IdHelper.getEdsResourceId(fhirResourceFiler.getServiceId(), ResourceType.ReferralRequest, "" + referralId);
            if (referralUuid == null) {
                //we have cases where we've received a referral status record without ever having received the referral
                //itself, in which case we can't do anything but ignore the status record
                //throw new Exception("Failed to find UUID for referral ID " + referralId);
                continue;
            }

            //find all episodes of care for the patient
            ResourceDalI resourceDal = DalProvider.factoryResourceDal();
            ReferralRequest referral = (ReferralRequest)resourceDal.getCurrentVersionAsResource(fhirResourceFiler.getServiceId(), ResourceType.ReferralRequest, referralUuid.toString());
            if (referral == null) {
                continue;
            }

            ReferralRequestBuilder referralRequestBuilder = new ReferralRequestBuilder(referral);
            if (addReferralStatuses(statuses, referralRequestBuilder)) {
                fhirResourceFiler.savePatientResource(null, false, referralRequestBuilder);
            }
        }
    }

    public void cacheReferralStatus(CsvCell referralIdCell, CsvCell dateCell, CsvCell referralStatusCell) {

        Long key = referralIdCell.getLong();

        List<ReferralStatusRecord> list = referralStatusRecords.get(key);
        if (list == null) {
            list = new ArrayList<>();
            referralStatusRecords.put(key, list);
        }
        list.add(new ReferralStatusRecord(dateCell, referralStatusCell));
    }

    public List<ReferralStatusRecord> getStatusesForReferral(CsvCell referralIdCell, ReferralRequestBuilder referralRequestBuilder) {
        Long key = referralIdCell.getLong();
        return referralStatusRecords.remove(key); //remove so we don't pick them up when calling processRemainingReferralStatuses(..)
    }

    public static boolean addReferralStatuses(List<ReferralStatusRecord> statuses, ReferralRequestBuilder referralRequestBuilder) throws Exception {

        if (statuses == null
                || statuses.isEmpty()) {
            return false;
        }

        ContainedListBuilder containedListBuilder = new ContainedListBuilder(referralRequestBuilder);

        //use a set for combining the existing and new stauts records, so duplicates automatically get removed
        Set<ReferralStatusWrapper> fullSet = new HashSet<>();

        //find any existing statuses on the episode
        List<List_.ListEntryComponent> items = containedListBuilder.getContainedListItems();
        if (items != null) {
            for (List_.ListEntryComponent item : items) {
                Date entryDate = item.getDate();
                CodeableConcept codeableConcept = item.getFlag();
                String statusDesc = codeableConcept.getText();

                fullSet.add(new ReferralStatusWrapper(entryDate, statusDesc));
            }
        }

        //add new/updates statuses
        for (ReferralStatusRecord status : statuses) {
            fullSet.add(new ReferralStatusWrapper(status));
        }

        //sort
        List<ReferralStatusWrapper> fullList = new ArrayList<>(fullSet);
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

        for (ReferralStatusWrapper wrapper : fullList) {

            CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(wrapper.getStatusDesc());
            containedListBuilder.addCodeableConcept(codeableConcept, wrapper.getStatusCell());
            if (wrapper.getDate() != null) {
                containedListBuilder.addDateToLastItem(wrapper.getDate(), wrapper.getDateCell());
            }
        }

        return true;
    }


    private static String findReferralStatusDesc(CsvCell referralStatusCell) throws Exception {
        TppConfigListOption tppConfigListOption = TppCsvHelper.lookUpTppConfigListOption(referralStatusCell);
        return tppConfigListOption.getListOptionName();
    }

    static class ReferralStatusWrapper {
        private ReferralStatusRecord cacheObject;
        private Date date; //may be null
        private String statusDesc;

        public ReferralStatusWrapper(ReferralStatusRecord cacheObject) throws Exception {
            this.cacheObject = cacheObject;
            this.date = cacheObject.getDateCell().getDate();
            this.statusDesc = findReferralStatusDesc(cacheObject.getReferralStatusCell());
        }

        public ReferralStatusWrapper(Date date, String statusDesc) {
            this.date = date;
            this.statusDesc = statusDesc;
        }

        public String getStatusDesc() {
            return statusDesc;
        }

        public Date getDate() {
            return date;
        }

        public CsvCell getStatusCell() {
            if (cacheObject != null) {
                return cacheObject.getReferralStatusCell();
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

            ReferralStatusWrapper that = (ReferralStatusWrapper) o;

            if (date != null ? !date.equals(that.date) : that.date != null) return false;
            return statusDesc != null ? statusDesc.equals(that.statusDesc) : that.statusDesc == null;

        }

        @Override
        public int hashCode() {
            int result = date != null ? date.hashCode() : 0;
            result = 31 * result + (statusDesc != null ? statusDesc.hashCode() : 0);
            return result;
        }
    }
}
