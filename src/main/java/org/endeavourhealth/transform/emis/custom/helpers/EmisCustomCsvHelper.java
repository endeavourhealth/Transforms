package org.endeavourhealth.transform.emis.custom.helpers;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.schema.RegistrationStatus;
import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.emis.custom.transforms.RegistrationStatusTransformer;
import org.hl7.fhir.instance.model.CodeableConcept;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class EmisCustomCsvHelper {
    private static final Logger LOG = LoggerFactory.getLogger(EmisCustomCsvHelper.class);

    private static ResourceDalI resourceDal = DalProvider.factoryResourceDal();

    private Map<String, List<RegStatusObj>> regStatusCache = new HashMap<>();
    private Map<String, CsvCell> regTypeCache = new HashMap<>();

    public void saveRegistrationStatues(FhirResourceFiler fhirResourceFiler) throws Exception {

        LOG.debug("Saving reg statuses for " + regStatusCache.size() + " patients");

        for (String patientKey: regStatusCache.keySet()) {
            List<RegStatusObj> list = regStatusCache.get(patientKey);

            //sort the list by processing ID
            list.sort((o1, o2) -> {
                return o1.processingOrder - o2.getProcessingOrder();
            });

            EpisodeOfCareBuilder episodeBuilder = findEpisodeBuilder(patientKey, fhirResourceFiler.getServiceId());

            //if the episode doesn't exist or was deleted, skip it
            if (episodeBuilder == null) {
                continue;
            }

            ContainedListBuilder containedListBuilder = new ContainedListBuilder(episodeBuilder);

            //remove existing statuses, since each time we get this file, it's a complete replacement
            containedListBuilder.removeContainedList();

            for (RegStatusObj obj: list) {

                CsvCell regStatusCell = obj.getRegStatusCell();
                RegistrationStatus registrationStatus = RegistrationStatusTransformer.convertRegistrationStatus(regStatusCell.getInt());
                CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(registrationStatus);
                boolean added = containedListBuilder.addCodeableConcept(codeableConcept, regStatusCell);
                if (added) {
                    CsvCell dateCell = obj.getDateTimeCell();
                    containedListBuilder.addDateToLastItem(dateCell.getDateTime(), dateCell);
                }
            }

            //carry over the registration type from this file if we've not got it on the episode
            if (!episodeBuilder.hasRegistrationType()) {
                CsvCell registrationTypeIdCell = regTypeCache.get(patientKey);
                RegistrationType registrationType = RegistrationStatusTransformer.convertRegistrationType(registrationTypeIdCell.getInt());
                episodeBuilder.setRegistrationType(registrationType, registrationTypeIdCell);
            }

            fhirResourceFiler.savePatientResource(null, false, episodeBuilder);
        }
    }

    private static EpisodeOfCareBuilder findEpisodeBuilder(String patientKey, UUID serviceId) throws Exception {
        //the patient GUID in the standard extract files is in upper case and
        //has curly braces around it, so we need to ensure this is the same
        String patientGuid = "{" + patientKey.toUpperCase() + "}";

        UUID globallyUniqueId = IdHelper.getEdsResourceId(serviceId, ResourceType.EpisodeOfCare, patientGuid);

        //if we've never heard of this patient before, skip it
        if (globallyUniqueId == null) {
            return null;
        }

        ResourceWrapper resourceHistory = resourceDal.getCurrentVersion(serviceId, ResourceType.EpisodeOfCare.toString(), globallyUniqueId);

        //if the resource has been deleted before, we'll have a null entry or one that says it's deleted
        if (resourceHistory == null
                || resourceHistory.isDeleted()) {
            return null;
        }

        String json = resourceHistory.getResourceData();
        EpisodeOfCare episodeOfCare = (EpisodeOfCare)FhirSerializationHelper.deserializeResource(json);
        return new EpisodeOfCareBuilder(episodeOfCare);
    }

    public void cacheRegStatus(CsvCell patientGuidCell, CsvCell regStatusCell, CsvCell regTypeCell, CsvCell dateCell, CsvCell processingOrderCell) {

        int processingOrder = processingOrderCell.getInt().intValue();
        RegStatusObj obj = new RegStatusObj(regStatusCell, dateCell, processingOrder);

        String key = patientGuidCell.getString();
        List<RegStatusObj> list = regStatusCache.get(key);
        if (list == null) {
            list = new ArrayList<>();
            regStatusCache.put(key, list);
        }
        list.add(obj);

        //and cache the reg type, which is the same for each reg status row
        regTypeCache.put(key, regTypeCell);
    }

    class RegStatusObj {
        private CsvCell regStatusCell;
        private CsvCell dateTimeCell;
        private int processingOrder;

        public RegStatusObj(CsvCell regStatusCell, CsvCell dateTimeCell, int processingOrder) {
            this.regStatusCell = regStatusCell;
            this.dateTimeCell = dateTimeCell;
            this.processingOrder = processingOrder;
        }

        public CsvCell getRegStatusCell() {
            return regStatusCell;
        }

        public CsvCell getDateTimeCell() {
            return dateTimeCell;
        }

        public int getProcessingOrder() {
            return processingOrder;
        }
    }

}
