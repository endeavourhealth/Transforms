package org.endeavourhealth.transform.emis.custom.helpers;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.schema.RegistrationStatus;
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

            ContainedListBuilder containedListBuilder = new ContainedListBuilder(episodeBuilder);

            //remove existing statuses, since each time we get this file, it's a complete replacement
            containedListBuilder.removeContainedList();

            for (RegStatusObj obj: list) {

                CsvCell regStatusCell = obj.getRegStatusCell();
                RegistrationStatus registrationStatus = convertRegistrationStatus(regStatusCell.getInt());
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


    private static RegistrationStatus convertRegistrationStatus(Integer obj) throws Exception {
        int value = obj.intValue();


        switch (value) {
            case 1:
                return RegistrationStatus.REGISTERED_PRESENTED;
            case 2:
                return RegistrationStatus.REGISTERED_MEDICAL_CARD_RECEIVED;
            case 3:
                return RegistrationStatus.REGISTERED_FP1_SUBMITTED;
            case 4:
                return RegistrationStatus.REGISTERED;
            case 5:
                return RegistrationStatus.REGISTERED_RECORD_SENT_FROM_FHSA;
            case 6:
                return RegistrationStatus.REGISTERED_RECORD_RECEIVED_FROM_FHSA;
            case 7:
                return RegistrationStatus.REGISTERED_LEFT_PRACTICE_STILL_REGISTERED;
            case 8:
                return RegistrationStatus.REGISTERED_CORRECTLY;
            case 9:
                return RegistrationStatus.REGISTERED_TEMPORARY_SHORT_STAY;
            case 10:
                return RegistrationStatus.REGISTERED_TEMPORARY_LONG_STAY;
            case 11:
                return RegistrationStatus.DEDUCTION_DEATH;
            case 12:
                return RegistrationStatus.DEDUCTION_DEATH_NOTIFICATION;
            case 13:
                return RegistrationStatus.DEDUCTION_RECORD_REQUESTED_BY_FHSA;
            case 14:
                return RegistrationStatus.REGISTERED_REMOVAL_TO_NEW_HA;
            case 15:
                return RegistrationStatus.REGISTERED_INTERNAL_TRANSFER;
            case 16:
                return RegistrationStatus.DEDUCTION_MENTAL_HOSPITAL;
            case 17:
                return RegistrationStatus.DEDUCTION_EMBARKATION;
            case 18:
                return RegistrationStatus.REGISTERED_NEW_HA_SAME_GP;
            case 19:
                return RegistrationStatus.DEDUCTION_ADOPTED_CHILD;
            case 20:
                return RegistrationStatus.DEDUCTION_SERVICES;
            case 21:
                return RegistrationStatus.DEDUCTION_AT_GP_REQUEST;
            case 22:
                return RegistrationStatus.DEDUCTION_REGISTRATION_CANCELLED;
            case 23:
                return RegistrationStatus.REGISTERED_SERVICES_DEPENDENT;
            case 24:
                return RegistrationStatus.DEDUCTION_AT_PATIENT_REQUEST;
            case 25:
                return RegistrationStatus.DEDUCTION_OTHER_REASON;
            case 26:
                return RegistrationStatus.DEDUCTION_MAIL_RETURNED_UNDELIVERED;
            case 27:
                return RegistrationStatus.REGISTERED_INTERNAL_TRANSFER_ADDRESS_CHANGE;
            case 28:
                return RegistrationStatus.REGISTERED_INTERNAL_TRANSFER_WITHIN_PARTNERSHIP;
            case 29:
                return RegistrationStatus.DEDUCTION_MAIL_STATES_GONE_AWAY;
            case 30:
                return RegistrationStatus.DEDUCTION_OUTSIDE_OF_AREA;
            case 31:
                return RegistrationStatus.DEDUCTION_NO_LONGER_RESIDENT;
            case 32:
                return RegistrationStatus.DEDUCTION_VIA_SCREENING_SYSTEM;
            case 33:
                return RegistrationStatus.DEDUCTION_VIA_VACCINATION_DATA;
            case 34:
                return RegistrationStatus.REGISTERED_REMOVAL_FROM_RESIDENTIAL_INSITUTE;
            case 35:
                return RegistrationStatus.DEDUCTION_RECORDS_SENT_BACK_TO_FHSA;
            case 36:
                return RegistrationStatus.DEDUCTION_RECORDS_RECEIVED_BY_FHSA;
            case 37:
                return RegistrationStatus.DEDUCTION_REGISTRATION_EXPIRED;
            default:
                throw new TransformException("Unsupported registration status " + value);
        }
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
