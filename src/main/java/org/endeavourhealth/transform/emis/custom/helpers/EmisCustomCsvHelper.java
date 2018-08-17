package org.endeavourhealth.transform.emis.custom.helpers;

import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
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

            ContainedListBuilder containedListBuilder = new ContainedListBuilder(episodeBuilder);

            //remove existing statuses, since each time we get this file, it's a complete replacement
            containedListBuilder.removeContainedList();

            for (RegStatusObj obj: list) {

                CsvCell regStatusCell = obj.getRegStatusCell();
                String registrationStatus = convertRegistrationStatus(regStatusCell.getInt());
                boolean added = containedListBuilder.addCodeableConcept(registrationStatus, regStatusCell);
                if (added) {
                    CsvCell dateCell = obj.getDateTimeCell();
                    containedListBuilder.addDateToLastItem(dateCell.getDateTime(), dateCell);
                }
            }

            //carry over the registration type from this file if we've not got it on the episode
            if (!episodeBuilder.hasRegistrationType()) {
                CsvCell registrationTypeIdCell = regTypeCache.get(patientKey);
                RegistrationType registrationType = convertRegistrationType(registrationTypeIdCell.getInt());
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


    private static String convertRegistrationStatus(Integer obj) throws Exception {
        int value = obj.intValue();

        switch (value) {
            case 1:
                return "Patient has presented";
            case 2:
                return "Medical card (FP4) received";
            case 3:
                return "Application Form FP1 submitted";
            case 4:
                return "Notification of registration";
            case 5:
                return "Medical record sent by FHSA";
            case 6:
                return "Record Received";
            case 7:
                return "Left Practice. Still Registered";
            case 8:
                return "Correctly registered";
            case 9:
                return "Short stay";
            case 10:
                return "Long stay";
            case 11:
                return "Death";
            case 12:
                return "Dead (Practice notification)";
            case 13:
                return "Record Requested by FHSA";
            case 14:
                return "Removal to New HA/HB";
            case 15:
                return "Internal transfer";
            case 16:
                return "Mental hospital";
            case 17:
                return "Embarkation";
            case 18:
                return "New HA/HB - same GP";
            case 19:
                return "Adopted child";
            case 20:
                return "Services";
            case 21:
                return "Deduction at GP's request";
            case 22:
                return "Registration cancelled";
            case 23:
                return "Service dependant";
            case 24:
                return "Deduction at patient's request";
            case 25:
                return "Other reason";
            case 26:
                return "Returned undelivered";
            case 27:
                return "Internal transfer - address change";
            case 28:
                return "Internal transfer within partnership";
            case 29:
                return "Correspondence states 'gone away'";
            case 30:
                return "Practice advise outside of area";
            case 31:
                return "Practice advise patient no longer resident";
            case 32:
                return "Practice advise removal via screening system";
            case 33:
                return "Practice advise removal via vaccination data";
            case 34:
                return "Removal from Residential Institute";
            case 35:
                return "Records sent back to FHSA";
            case 36:
                return "Records received by FHSA";
            case 37:
                return "Registration expired";
            default:
                throw new TransformException("Unsupported registration status " + value);
        }

        /*
        NOTE: the below are also registration statuses, but no known patients have them
        38	All records removed
        39	Untraced-outwith HB
        40	Multiple Transfer
        41	Intra-consortium transfer
        42	District birth
        43	Transfer in
        44	Transfer out
        45	Movement in
        46	Movement out
        47	Died
        48	Still birth
        49	Living out, treated in
        50	Living in, treated out

         */

    }

    private static RegistrationType convertRegistrationType(Integer obj) throws Exception {
        int value = obj.intValue();

        if (value == 1) { //Emergency
            return RegistrationType.EMERGENCY;
        } else if (value == 2) { //Immediately Necessary
            return RegistrationType.IMMEDIATELY_NECESSARY;
        } else if (value == 3) { //Private
            return RegistrationType.PRIVATE;
        } else if (value == 4) { //Regular
            return RegistrationType.REGULAR_GMS;
        } else if (value == 5) { //Temporary
            return RegistrationType.TEMPORARY;
        } else if (value == 6) { //Community Registered
            return RegistrationType.COMMUNITY;
        } else if (value == 7) { //Dummy
            return RegistrationType.DUMMY;
        } else if (value == 8) { //Other
            return RegistrationType.OTHER;
        } else if (value == 12) { //Walk-In Patient
            return RegistrationType.WALK_IN;
        } else if (value == 13) { //Minor Surgery
            return RegistrationType.MINOR_SURGERY;
        } else if (value == 11) { //Child Health Services
            return RegistrationType.CHILD_HEALTH_SURVEILLANCE;
        } else if (value == 9) { //Contraceptive Services
            return RegistrationType.CONTRACEPTIVE_SERVICES;
        } else if (value == 10) { //Maternity Services
            return RegistrationType.MATERNITY_SERVICES;
        } else if (value == 16) { //Yellow Fever
            return RegistrationType.YELLOW_FEVER;
        } else if (value == 15) { //Pre Registration
            return RegistrationType.PRE_REGISTRATION;
        } else if (value == 14) { //Sexual Health
            return RegistrationType.SEXUAL_HEALTH;
        } else if (value == 24) { //Vasectomy
            return RegistrationType.VASECTOMY;
        } else if (value == 28) { //Out of Hours
            return RegistrationType.OUT_OF_HOURS;
        } else {
            throw new TransformException("Unsupported registration type " + value);
        }

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
