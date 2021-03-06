package org.endeavourhealth.transform.subscriber.transforms;

import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.RegistrationStatus;
import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformHelper;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;
import org.endeavourhealth.transform.subscriber.targetTables.RegistrationStatusHistory;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

public class EpisodeOfCareTransformer extends AbstractSubscriberTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(EpisodeOfCareTransformer.class);

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.EpisodeOfCare;
    }

    public boolean shouldAlwaysTransform() {
        return true;
    }

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.EpisodeOfCare model = params.getOutputContainer().getEpisodesOfCare();

        EpisodeOfCare fhir = (EpisodeOfCare)resourceWrapper.getResource(); //returns null if deleted
        List<ResourceWrapper> fullHistory = EnterpriseTransformHelper.getFullHistory(resourceWrapper);

        //if deleted, confidential or the entire patient record shouldn't be there, then delete
        if (resourceWrapper.isDeleted()
                //|| isConfidential(fhir)
                || params.getShouldPatientRecordBeDeleted()) {

            deleteRegistrationStatusHistory(fullHistory, resourceWrapper, params);
            model.writeDelete(subscriberId);
            return;
        }

        long id;
        long organizationId;
        long patientId;
        long personId;
        Integer registrationTypeConceptId = null;
        Integer registrationStatusConceptId = null;
        Date dateRegistered = null;
        Date dateRegisteredEnd = null;
        Long usualGpPractitionerId = null;
        //Long managingOrganisationId = null;

        id = subscriberId.getSubscriberId();
        organizationId = params.getSubscriberOrganisationId().longValue();
        patientId = params.getSubscriberPatientId().longValue();
        personId = params.getSubscriberPersonId().longValue();

        if (fhir.hasCareManager()) {
            Reference practitionerReference = fhir.getCareManager();
            usualGpPractitionerId = transformOnDemandAndMapId(practitionerReference, SubscriberTableId.PRACTITIONER, params);
        }

        //registration type has moved to the EpisodeOfCare resource, although there will be some old instances (for now)
        //where the extension is on the Patient resource
        Extension regTypeExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.EPISODE_OF_CARE_REGISTRATION_TYPE);
        if (regTypeExtension == null) {
            //if not on the episode, check the patientPatient fhirPatient = (Patient)findResource(fhirEpisode.getPatient(), params);
            Patient fhirPatient = (Patient)params.findOrRetrieveResource(fhir.getPatient());
            if (fhirPatient != null) { //if a patient has been subsequently deleted, this will be null)
                regTypeExtension = ExtensionConverter.findExtension(fhirPatient, FhirExtensionUri.EPISODE_OF_CARE_REGISTRATION_TYPE);
            }
        }

        if (regTypeExtension != null) {
            Coding coding = (Coding)regTypeExtension.getValue();
            RegistrationType fhirRegistrationType = RegistrationType.fromCode(coding.getCode());

            registrationTypeConceptId = IMHelper.getIMConcept(params, fhir, IMConstant.FHIR_REGISTRATION_TYPE, fhirRegistrationType.getCode(), coding.getDisplay());
        }

        //reg status is stored in a contained list with an extension giving the internal reference to it
        List<RegStatus> regStatuses = getRegStatusList(fhir);
        if (!regStatuses.isEmpty()) {
            RegStatus latest = regStatuses.get(regStatuses.size()-1);
            RegistrationStatus status = latest.getStatus();
            registrationStatusConceptId = IMHelper.getIMConcept(params, fhir, IMConstant.FHIR_REGISTRATION_STATUS, status.getCode(), status.getDescription());
        }


        Period period = fhir.getPeriod();
        if (period.hasStart()) {
            dateRegistered = period.getStart();
        }
        if (period.hasEnd()) {
            dateRegisteredEnd = period.getEnd();
        }

        model.writeUpsert(subscriberId,
                organizationId,
                patientId,
                personId,
                registrationTypeConceptId,
                registrationStatusConceptId,
                dateRegistered,
                dateRegisteredEnd,
                usualGpPractitionerId);

        transformRegistrationStatusHistory(organizationId, patientId, personId, id, fhir, resourceWrapper, params, fullHistory);
    }

    @Override
    protected SubscriberTableId getMainSubscriberTableId() {
        return SubscriberTableId.EPISODE_OF_CARE;
    }



    private void deleteRegistrationStatusHistory(List<ResourceWrapper> fullHistory, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        RegistrationStatusHistory writer = params.getOutputContainer().getRegistrationStatusHistory();
        Set<EpisodeOfCareTransformer.RegStatus> regStatuses = EpisodeOfCareTransformer.getAllRegStatuses(fullHistory);

        //generate IDs
        Map<EpisodeOfCareTransformer.RegStatus, SubscriberId> hmIds = EpisodeOfCareTransformer.findRegStatusIds(regStatuses, params.getSubscriberConfigName(), false);

        for (EpisodeOfCareTransformer.RegStatus regStatus: regStatuses) {

            SubscriberId subTableId = hmIds.get(regStatus);
            if (subTableId != null) { //may be null if never transformed before
                writer.writeDelete(subTableId);
            }
        }
    }

    private static void transformRegistrationStatusHistory(long organisationId, long patientId, long personId,
                                                           long episodeOfCareId, EpisodeOfCare episodeOfCare,
                                                           ResourceWrapper resourceWrapper, SubscriberTransformHelper params,
                                                           List<ResourceWrapper> fullHistory) throws  Exception {

        RegistrationStatusHistory writer = params.getOutputContainer().getRegistrationStatusHistory();

        List<EpisodeOfCareTransformer.RegStatus> regStatuses = EpisodeOfCareTransformer.getRegStatusList(episodeOfCare);
        Set<EpisodeOfCareTransformer.RegStatus> fullHistoryRegStatuses = EpisodeOfCareTransformer.getAllRegStatuses(fullHistory);

        //generate IDs
        Map<EpisodeOfCareTransformer.RegStatus, SubscriberId> hmIds = EpisodeOfCareTransformer.findRegStatusIds(regStatuses, params.getSubscriberConfigName(), true);

        for (int i=0; i<regStatuses.size(); i++) {
            EpisodeOfCareTransformer.RegStatus regStatus = regStatuses.get(i);

            Date startDate = regStatus.getStart();

            RegistrationStatus status = regStatus.getStatus();
            Integer registrationStatusConceptId = IMHelper.getIMConcept(params, episodeOfCare, IMConstant.FHIR_REGISTRATION_STATUS, status.getCode(), status.getDescription());

            //get the subsequent status to derive the end date of this one.
            //if there is no subsequent status then this is the most recent status and has no end date
            Date endDate = null;
            if (i+1 < regStatuses.size()) {
                EpisodeOfCareTransformer.RegStatus nextRegStatus = regStatuses.get(i+1);
                endDate = nextRegStatus.getStart();
            }

            //create a unique Id mapping reference for this episode of care registration status using
            //code and date. Sometimes duplicates are sent which we will simply overwrite/upsert
            SubscriberId subTableId = hmIds.get(regStatus);

            writer.writeUpsert(subTableId,
                    organisationId,
                    patientId,
                    personId,
                    episodeOfCareId,
                    registrationStatusConceptId,
                    startDate,
                    endDate);
        }

        //we need to DELETE any reg status records no longer found on our episode
        fullHistoryRegStatuses.removeAll(regStatuses); //take away the current ones

        Map<EpisodeOfCareTransformer.RegStatus, SubscriberId> fullHistoryHmIds = EpisodeOfCareTransformer.findRegStatusIds(fullHistoryRegStatuses, params.getSubscriberConfigName(), false);

        for (EpisodeOfCareTransformer.RegStatus regStatus: fullHistoryRegStatuses) {
            SubscriberId subTableId = fullHistoryHmIds.get(regStatus);
            if (subTableId != null) {
                writer.writeDelete(subTableId);
            }
        }
    }

    /**
     * finds (and creates) mapped IDs for each reg status record
     */
    public static Map<EpisodeOfCareTransformer.RegStatus, SubscriberId> findRegStatusIds(Collection<EpisodeOfCareTransformer.RegStatus> regStatuses,
                                                                                          String subscriberConfigName,
                                                                                          boolean createIfMissing) throws Exception {

        if (regStatuses.isEmpty()) {
            return new HashMap<>();
        }

        //pre-prepare a list of all source IDs that need mapping
        Map<String, EpisodeOfCareTransformer.RegStatus> hmBySourceId = new HashMap<>();
        List<String> sourceIds = new ArrayList<>();
        for (EpisodeOfCareTransformer.RegStatus regStatus: regStatuses) {
            String sourceId = regStatus.generateUniqueId();
            sourceIds.add(sourceId);
            hmBySourceId.put(sourceId, regStatus);
        }

        //map all in one go
        Map<String, SubscriberId> hmIds = findOrCreateSubscriberIds(subscriberConfigName, SubscriberTableId.REGISTRATION_STATUS_HISTORY, sourceIds, createIfMissing);

        //revserve look up to return an ID for each status
        Map<EpisodeOfCareTransformer.RegStatus, SubscriberId> ret = new HashMap<>();
        for (String sourceId: hmIds.keySet()) {
            SubscriberId id = hmIds.get(sourceId);
            EpisodeOfCareTransformer.RegStatus regStatus = hmBySourceId.get(sourceId);
            ret.put(regStatus, id);
        }

        return ret;
    }
    
    /**
     * returns the list of reg statuses, in date order (earliest to latest)
     */
    public static List<RegStatus> getRegStatusList(EpisodeOfCare episodeOfCare) throws Exception {

        List<RegStatus> ret = new ArrayList<>();

        List_ list = ContainedListBuilder.findContainedList(episodeOfCare, FhirExtensionUri.EPISODE_OF_CARE_REGISTRATION_STATUS);
        if (list == null) {
            return ret;
        }

        List<List_.ListEntryComponent> entries = list.getEntry();
        HashSet<String> uniqueEntries = new HashSet<>();
        for (List_.ListEntryComponent entry: entries) {
            if (entry.hasFlag()) {
                CodeableConcept codeableConcept = entry.getFlag();
                String code = CodeableConceptHelper.findCodingCode(codeableConcept, FhirValueSetUri.VALUE_SET_REGISTRATION_STATUS);
                RegistrationStatus status = RegistrationStatus.fromCode(code);
                Date d = entry.getDate();
                RegStatus regStatus = new RegStatus(episodeOfCare, status, d);
                String id = regStatus.generateUniqueId();
                if (!uniqueEntries.contains(id)) {
                    uniqueEntries.add(id);
                    ret.add(regStatus);
                }
            }
        }

        return ret;
    }

    /**
     * returns all the distinct reg statuses for all instances of the episode. Note they are NOT in any order.
     */
    public static Set<RegStatus> getAllRegStatuses(List<ResourceWrapper> fullHistory) throws Exception {

        Set<RegStatus> ret = new HashSet<>();

        for (ResourceWrapper wrapper: fullHistory) {

            //all the registration status Id values are in this extension
            //ADT data has no registration history and occasionally resourcedata is null, so check for that. SD-150.
            if (!wrapper.isDeleted() && wrapper.getResourceData() != null) {
                EpisodeOfCare episodeOfCare = (EpisodeOfCare) wrapper.getResource();
                List<RegStatus> statuses = getRegStatusList(episodeOfCare);
                for (RegStatus status : statuses) {
                    ret.add(status);
                }
            }
        }

        return ret;
    }


    public static class RegStatus {

        private String owningEpisodeReferenceStr;
        private RegistrationStatus status;
        private Date start;
        private String cachedUniqueId = null;

        public RegStatus(EpisodeOfCare episodeOfCare, RegistrationStatus status, Date start) {
            this.owningEpisodeReferenceStr = ReferenceHelper.createReferenceExternal(episodeOfCare).getReference();
            this.status = status;
            this.start = start;
        }

        public RegistrationStatus getStatus() {
            return status;
        }

        public Date getStart() {
            return start;
        }

        /**
         * generates a unique ID for mapping to a subscriber ID, based on reg status and date
         */
        public String generateUniqueId() {

            if (cachedUniqueId == null) {
                StringBuilder sb = new StringBuilder();
                sb.append(owningEpisodeReferenceStr);
                sb.append("-REGSTATUS-");
                sb.append(status.getCode());
                if (start != null) {
                    sb.append("-");
                    sb.append(new SimpleDateFormat("yyyyMMddHHmmss").format(start));
                }
                cachedUniqueId = sb.toString();
            }
            return cachedUniqueId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            RegStatus regStatus = (RegStatus) o;

            if (!owningEpisodeReferenceStr.equals(regStatus.owningEpisodeReferenceStr)) return false;
            if (status != regStatus.status) return false;
            return start != null ? start.equals(regStatus.start) : regStatus.start == null;

        }

        @Override
        public int hashCode() {
            int result = owningEpisodeReferenceStr.hashCode();
            result = 31 * result + status.hashCode();
            result = 31 * result + (start != null ? start.hashCode() : 0);
            return result;
        }
    }
}

