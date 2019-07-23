package org.endeavourhealth.transform.tpp.csv.helpers;

import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.common.fhir.schema.RegistrationStatus;
import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherCommon.*;
import org.endeavourhealth.core.database.dal.publisherCommon.models.*;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.TppConfigListOptionDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.dal.publisherTransform.models.TppConfigListOption;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.referenceLists.ReferenceList;
import org.endeavourhealth.transform.common.referenceLists.ReferenceListNoCsvCells;
import org.endeavourhealth.transform.common.referenceLists.ReferenceListSingleCsvCells;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ResourceBuilderBase;
import org.endeavourhealth.transform.tpp.cache.*;
import org.endeavourhealth.transform.tpp.csv.transforms.patient.SRPatientRegistrationTransformer;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

public class TppCsvHelper implements HasServiceSystemAndExchangeIdI {
    private static final Logger LOG = LoggerFactory.getLogger(TppCsvHelper.class);

    private static final String OPERATIONS_PROCEDURES = "X0001";
    private static final String DISORDERS = "X0003";
    private static final String FAMILY_HISTORY_DISORDERS = "Xa1px";
    private static final String ALLERGIC_DISORDER = "Xa1pQ";
    private static final ParserPool PARSER_POOL = new ParserPool();

    public static final String ADMIN_CACHE_KEY = "TPP";

    private static TppMappingRefDalI tppMappingRefDalI = DalProvider.factoryTppMappingRefDal();
    private static TppConfigListOptionDalI tppConfigListOptionDalI = DalProvider.factoryTppConfigListOptionDal();
    private static TppImmunisationContentDalI tppImmunisationContentDalI = DalProvider.factoryTppImmunisationContentDal();
    private static InternalIdDalI internalIdDal = DalProvider.factoryInternalIdDal();
    private static TppMultiLexToCtv3MapDalI multiLexToCTV3MapDalI = DalProvider.factoryTppMultiLexToCtv3MapDal();
    private static TppCtv3LookupDalI tppCtv3LookupRefDal = DalProvider.factoryTppCtv3LookupDal();
    private static TppCtv3HierarchyRefDalI ctv3HierarchyRefDalI = DalProvider.factoryTppCtv3HierarchyRefDal();

    private static Map<String, TppMappingRef> tppMappingRefs = new ConcurrentHashMap<>();
    private static Map<String, TppConfigListOption> tppConfigListOptions = new ConcurrentHashMap<>();
    private static Map<String, TppImmunisationContent> tppImmunisationContents = new ConcurrentHashMap<>();
    private static Map<String, String> internalIdMapCache = new ConcurrentHashMap<>();
    private static Map<String, TppMultiLexToCtv3Map> multiLexToCTV3Map = new ConcurrentHashMap<>();
    private static Map<String, TppCtv3Lookup> tppCtv3Lookups = new ConcurrentHashMap<>();

    private Map<Long, ReferenceList> consultationNewChildMap = new ConcurrentHashMap<>();
    private Map<Long, ReferenceList> consultationExistingChildMap = new ConcurrentHashMap<>();
    private Map<Long, ReferenceList> encounterAppointmentOrVisitMap = new ConcurrentHashMap<>();
    private Map<Long, List<MedicalRecordStatusCacheObject>> medicalRecordStatusMap = new ConcurrentHashMap<>();
    private StaffMemberCache staffMemberCache = new StaffMemberCache();
    private AppointmentFlagCache appointmentFlagCache = new AppointmentFlagCache();
    private PatientResourceCache patientResourceCache = new PatientResourceCache();
    private ConditionResourceCache conditionResourceCache = new ConditionResourceCache();
    private ReferralRequestResourceCache referralRequestResourceCache = new ReferralRequestResourceCache();
    private Map<Long, String> problemReadCodes = new HashMap<>();
//    private Map<String, String> allergyReadCodes = new HashMap<>();
    private Map<Long, DateAndCode> ethnicityMap = new HashMap<>();
    private Map<Long, DateAndCode> maritalStatusMap = new HashMap<>();
    private Map<String, EthnicCategory> knownEthnicCodes = new HashMap<>();
    private ArrayList<String> ctv3EthnicCodes = new ArrayList<>();
    //    private ArrayList<String> ctv3ProcedureCodes = new ArrayList();
//    private ArrayList<String> ctv3DisorderCodes = new ArrayList();
//    private ArrayList<String> ctv3FamilyDisorderCodes = new ArrayList();;
    private Map<String, Long> staffMemberToProfileMap = new HashMap<>();
    private ThreadPool utilityThreadPool = null;
    private Map<String, ResourceType> codeToTypes = new HashMap<>();

    private final UUID serviceId;
    private final UUID systemId;
    private final UUID exchangeId;

    public TppCsvHelper(UUID serviceId, UUID systemId, UUID exchangeId) {
        this.serviceId = serviceId;
        this.systemId = systemId;
        this.exchangeId = exchangeId;
        buildKnownEthnicCodes();
    }

    private ResourceDalI resourceRepository = DalProvider.factoryResourceDal();

    public Reference createOrganisationReference(CsvCell organizationGuid) {
        return ReferenceHelper.createReference(ResourceType.Organization, organizationGuid.getString());
    }

    public Reference createOrganisationReference(String organizationGuid) {
        return ReferenceHelper.createReference(ResourceType.Organization, organizationGuid);
    }

    public Reference createLocationReference(CsvCell locationGuid) {
        return ReferenceHelper.createReference(ResourceType.Location, locationGuid.getString());
    }

    public Reference createPatientReference(CsvCell patientGuid) {
        return ReferenceHelper.createReference(ResourceType.Patient, patientGuid.getString());
    }

    public Reference createPractitionerReferenceForProfileId(CsvCell staffProfileIdCell) {
        return ReferenceHelper.createReference(ResourceType.Practitioner, staffProfileIdCell.getString());
    }

    public Reference createPractitionerReferenceForStaffMemberId(CsvCell staffMemberIdCell, CsvCell profileIdRecordedBy, CsvCell organisationDoneAtCell) throws Exception {
        Long profileId = findStaffProfileIdForStaffMemberId(staffMemberIdCell, profileIdRecordedBy, organisationDoneAtCell);
        return ReferenceHelper.createReference(ResourceType.Practitioner, "" + profileId);
    }

    public Reference createObservationReference(String observationGuid, String patientGuid) {
        return ReferenceHelper.createReference(ResourceType.Observation, patientGuid + ":" + observationGuid);
    }

    public Long findStaffProfileIdForStaffMemberId(CsvCell staffMemberIdCell, CsvCell profileIdRecordedBy, CsvCell organisationDoneAtCell) throws Exception {

        //Practitioner resources use the profile ID as the source ID, so need to look up an ID for our staff member
        String cacheKey = staffMemberIdCell.getString() + "/" + profileIdRecordedBy.getString() + "/" + organisationDoneAtCell.getString();
        Long profileId = staffMemberToProfileMap.get(cacheKey);
        if (profileId == null) {

            List<InternalIdMap> mappings = internalIdDal.getSourceId(serviceId, InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, staffMemberIdCell.getString());
            if (mappings.isEmpty()) {
                throw new TransformException("Failed to find any staff profile IDs for staff member ID " + staffMemberIdCell.getString());
            }

            //our staff member is likely to have multiple role profiles, so we use the profile ID recorded by and organisation
            //to narrow it down to the correct one, since 99% of the time, the person who recorded the consultation actually did the consultation

            //if one of the profiles for the staff member is the same as recorded the consultation, then that's the one to us
            if (!profileIdRecordedBy.isEmpty()) {
                for (InternalIdMap mapping : mappings) {
                    if (mapping.getSourceId().equals(profileIdRecordedBy.getString())) {
                        profileId = Long.valueOf(mapping.getSourceId());
                        break;
                    }
                }
            }

            //if we know the organisation is was done at, we can try to use that to narrow down the profile ID
            if (profileId == null
                    && !organisationDoneAtCell.isEmpty()) {
                for (InternalIdMap mapping : mappings) {

                    String mappingProfileId = mapping.getSourceId();

                    //note that we don't save practitioners to the EHR database until needed, so we have to use the admin cache
                    //as a source for the UNMAPPED practitioner data
                    EmisTransformDalI dal = DalProvider.factoryEmisTransformDal();
                    EmisAdminResourceCache adminResourceObj = dal.getAdminResource(TppCsvHelper.ADMIN_CACHE_KEY, ResourceType.Practitioner, mappingProfileId);
                    if (adminResourceObj == null) {
                        continue;
                    }

                    //note this practitioner is NOT ID mapped
                    Practitioner practitioner = (Practitioner) FhirSerializationHelper.deserializeResource(adminResourceObj.getResourceData());
                    if (practitioner.hasPractitionerRole()) {

                        Practitioner.PractitionerPractitionerRoleComponent role = practitioner.getPractitionerRole().get(0);
                        if (role.hasManagingOrganization()) {
                            Reference orgReference = role.getManagingOrganization();
                            String sourceOrgId = ReferenceHelper.getReferenceId(orgReference);
                            if (sourceOrgId.equalsIgnoreCase(organisationDoneAtCell.getString())) {
                                profileId = Long.valueOf(mapping.getSourceId());
                                break;
                            }
                        }
                    }
                }
            }

            //if we still can't work out which profile it is, fall back on using the first
            if (profileId == null) {
                InternalIdMap mapping = mappings.get(0);
                profileId = Long.valueOf(mapping.getSourceId());
            }

            staffMemberToProfileMap.put(cacheKey, profileId);
        }

        return profileId;
    }

    /*public Reference createPractitionerReference(CsvCell practitionerGuid) {
        return ReferenceHelper.createReference(ResourceType.Practitioner, practitionerGuid.getString());
    }

    public Reference createPractitionerReference(String practitionerGuid) {
        return ReferenceHelper.createReference(ResourceType.Practitioner, practitionerGuid);
    }*/

    public Reference createScheduleReference(CsvCell scheduleGuid) {
        return ReferenceHelper.createReference(ResourceType.Schedule, scheduleGuid.getString());
    }

    public Reference createSlotReference(CsvCell slotGuid) {
        return ReferenceHelper.createReference(ResourceType.Slot, slotGuid.getString());
    }

    public Reference createConditionReference(CsvCell problemGuid) {
        return ReferenceHelper.createReference(ResourceType.Condition, problemGuid.getString());
    }

    public Reference createMedicationStatementReference(CsvCell medicationStatementGuid) {
        return ReferenceHelper.createReference(ResourceType.MedicationStatement, medicationStatementGuid.getString());
    }

    public Reference createEncounterReference(CsvCell encounterGuid) {
        return ReferenceHelper.createReference(ResourceType.Encounter, encounterGuid.getString());
    }

    public Reference createEpisodeReference(String episodeId) {
        //the episode of care just uses the patient GUID as its ID, so that's all we need to refer to it too
        return ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeId);
    }

    public static void setUniqueId(ResourceBuilderBase resourceBuilder, CsvCell sourceGuid) {
        resourceBuilder.setId(sourceGuid.getString(), sourceGuid);
    }


    public Resource retrieveResource(String locallyUniqueId, ResourceType resourceType) throws Exception {

        UUID globallyUniqueId = IdHelper.getEdsResourceId(serviceId, resourceType, locallyUniqueId);

        //if we've never mapped the local ID to a EDS UI, then we've never heard of this resource before
        if (globallyUniqueId == null) {
            return null;
        }

        ResourceWrapper resourceHistory = resourceRepository.getCurrentVersion(serviceId, resourceType.toString(), globallyUniqueId);

        //if the resource has been deleted before, we'll have a null entry or one that says it's deleted
        if (resourceHistory == null
                || resourceHistory.isDeleted()) {
            return null;
        }

        String json = resourceHistory.getResourceData();
        return PARSER_POOL.parse(json);
    }


    public class DateAndCode {
        private DateTimeType date = null;
        private CodeableConcept codeableConcept = null;

        public DateAndCode(DateTimeType date, CodeableConcept codeableConcept) {
            this.date = date;
            this.codeableConcept = codeableConcept;
        }

        public DateTimeType getDate() {
            return date;
        }

        public CodeableConcept getCodeableConcept() {
            return codeableConcept;
        }

        public boolean isBefore(DateTimeType other) {
            if (date == null) {
                return true;
            } else if (other == null) {
                return false;
            } else {
                return date.before(other);
            }

        }
    }

    public void cacheNewConsultationChildRelationship(CsvCell consultationGuid,
                                                      String resourceGuid,
                                                      ResourceType resourceType) {

        if (consultationGuid.isEmpty()) {
            return;
        }

        Long consultationLocalUniqueId = consultationGuid.getLong();
        ReferenceList list = consultationNewChildMap.get(consultationLocalUniqueId);
        if (list == null) {
            //we know there will only be a single CsvCell so use this implementation to save memory
            list = new ReferenceListSingleCsvCells();
            //list = new ReferenceList();
            consultationNewChildMap.put(consultationLocalUniqueId, list);
        }

        Reference resourceReference = ReferenceHelper.createReference(resourceType, resourceGuid);
        list.add(resourceReference, consultationGuid);
    }

    public ReferenceList getAndRemoveNewConsultationRelationships(CsvCell consultationIdCell) {
        return consultationNewChildMap.remove(consultationIdCell.getLong());
    }

    public void cacheNewEncounterAppointmentOrVisitMap(CsvCell encounterId,
                                                       String resourceGuid,
                                                       ResourceType resourceType) {

        if (encounterId.isEmpty()) {
            return;
        }

        Long consultationLocalUniqueId = encounterId.getLong();
        ReferenceList list = encounterAppointmentOrVisitMap.get(consultationLocalUniqueId);
        if (list == null) {
            //we know there will only be a single CsvCell so use this implementation to save memory
            list = new ReferenceListSingleCsvCells();
            //list = new ReferenceList();
            encounterAppointmentOrVisitMap.put(consultationLocalUniqueId, list);
        }

        String resourceLocalUniqueId = resourceGuid;
        Reference resourceReference = ReferenceHelper.createReference(resourceType, resourceLocalUniqueId);
        list.add(resourceReference, encounterId);
    }

    public ReferenceList getAndRemoveEncounterAppointmentOrVisitMap(String encounterSourceId) {
        return encounterAppointmentOrVisitMap.remove(encounterSourceId);
    }

    public void cacheConsultationPreviousLinkedResources(CsvCell consultationIdCell, List<Reference> previousReferences) {

        if (previousReferences == null
                || previousReferences.isEmpty()) {
            return;
        }

        //we know there will be no CsvCells so use this implementation to save memory
        ReferenceList obj = new ReferenceListNoCsvCells();
        //ReferenceList obj = new ReferenceList();
        obj.add(previousReferences);

        consultationExistingChildMap.put(consultationIdCell.getLong(), obj);
    }

    public ReferenceList findConsultationPreviousLinkedResources(CsvCell consultationIdCell) {
        return consultationExistingChildMap.remove(consultationIdCell.getLong());
    }

    public void cacheProblemObservationGuid(CsvCell problemGuid, String readCode) {
        problemReadCodes.put(problemGuid.getLong(), readCode);
    }

    public boolean isProblemObservationGuid(CsvCell problemGuid) {
        return problemReadCodes.containsKey(problemGuid.getLong());

        //TODO - what if we're processing an UPDATED SRCode record without an updated SRPRoblem record in the same extract?
        //This code needs to check the DB to see if the existing observation is already saved as a problem
    }

    public void cacheMedicalRecordStatus(CsvCell patientGuid, CsvCell dateCell, CsvCell medicalRecordStatusCell) {

        Long key = patientGuid.getLong();
        List<MedicalRecordStatusCacheObject> list = medicalRecordStatusMap.get(key);
        if (list == null) {
            list = new ArrayList<>();
            medicalRecordStatusMap.put(key, list);
        }
        list.add(new MedicalRecordStatusCacheObject(dateCell, medicalRecordStatusCell));
    }

    public List<MedicalRecordStatusCacheObject> getAndRemoveMedicalRecordStatus(CsvCell patientGuid) {
        Long key = patientGuid.getLong();
        return medicalRecordStatusMap.remove(key);
    }

    public void processRemainingRegistrationStatuses(FhirResourceFiler fhirResourceFiler) throws Exception {

        for (Long patientId : medicalRecordStatusMap.keySet()) {
            List<MedicalRecordStatusCacheObject> statusForPatient = medicalRecordStatusMap.get(patientId);

            EpisodeOfCare episodeOfCare = (EpisodeOfCare) retrieveResource("" + patientId, ResourceType.EpisodeOfCare);
            if (episodeOfCare == null) {
                continue;
            }

            EpisodeOfCareBuilder episodeBuilder = new EpisodeOfCareBuilder(episodeOfCare);
            ContainedListBuilder containedListBuilder = new ContainedListBuilder(episodeBuilder);

            for (MedicalRecordStatusCacheObject status : statusForPatient) {

                CsvCell statusCell = status.getStatusCell();
                RegistrationStatus medicalRecordStatus = SRPatientRegistrationTransformer.convertMedicalRecordStatus(statusCell);
                CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(medicalRecordStatus);
                containedListBuilder.addCodeableConcept(codeableConcept, statusCell);

                CsvCell dateCell = status.getDateCell();
                if (!dateCell.isEmpty()) {
                    containedListBuilder.addDateToLastItem(dateCell.getDateTime(), dateCell);
                }
            }

            fhirResourceFiler.savePatientResource(null, false, episodeBuilder);
        }
    }

//    public void cacheAllergyCode(String readCode, String readTerm) {
//        allergyReadCodes.put(readCode, readTerm);
//    }

//    public boolean isAllergyCode(String readCode, String readTerm) throws Exception {
//
//        // check cache first
//        if (codeToTypes.get(readCode).equals(ResourceType.AllergyIntolerance)) {
//            return true;
//        }
//
//        // check db and cache if true
//        boolean isAllergy
//                = ctv3HierarchyRefDalI.isChildCodeUnderParentCode(readCode, ALLERGIC_DISORDER);
//        if (isAllergy) {
//            codeToTypes.put(readCode, ResourceType.AllergyIntolerance);
//            cacheAllergyCode(readCode, readTerm);
//            return true;
//        }
//
//        // otherwise, it's not an allergy code
//        return false;
//    }

    public boolean isEthnicityCode(String readCode) throws Exception {
        if (knownEthnicCodes.containsKey(readCode)) {
            return true;
        } else {
            //return ctv3HierarchyRefDalI.isChildCodeUnderParentCode(readCode, ETHNICITY_ROOT);
            return this.ctv3EthnicCodes.contains(readCode);
        }
    }

    public EthnicCategory getKnownEthnicCategory(String readcode) {
        if (knownEthnicCodes.containsKey(readcode)) {
            return knownEthnicCodes.get(readcode);
        }
        return null;
    }

    public void cacheEthnicity(CsvCell patientGuid, DateTimeType fhirDate, EthnicCategory ethnicCategory) {
        DateAndCode dc = ethnicityMap.get(patientGuid.getLong());
        if (dc == null
                || dc.isBefore(fhirDate)) {
            ethnicityMap.put(patientGuid.getLong(), new DateAndCode(fhirDate, CodeableConceptHelper.createCodeableConcept(ethnicCategory)));
        }
    }

    public CodeableConcept findEthnicity(CsvCell patientGuid) {
        DateAndCode dc = ethnicityMap.remove(patientGuid.getLong());
        if (dc != null) {
            return dc.getCodeableConcept();
        } else {
            return null;
        }
    }

    public void cacheMaritalStatus(CsvCell patientGuid, DateTimeType fhirDate, MaritalStatus maritalStatus) {
        DateAndCode dc = maritalStatusMap.get(patientGuid.getLong());
        if (dc == null
                || dc.isBefore(fhirDate)) {
            maritalStatusMap.put(patientGuid.getLong(), new DateAndCode(fhirDate, CodeableConceptHelper.createCodeableConcept(maritalStatus)));
        }
    }

    public CodeableConcept findMaritalStatus(CsvCell patientGuid) {
        DateAndCode dc = maritalStatusMap.remove(patientGuid);
        if (dc != null) {
            return dc.getCodeableConcept();
        } else {
            return null;
        }
    }

    // Lookup code reference from SRMapping generated db
    public TppMappingRef lookUpTppMappingRef(CsvCell cell) throws Exception {

        Long rowId = cell.getLong();
        String codeLookup = rowId.toString();

        //Find the code in the cache
        TppMappingRef tppMappingRefFromCache = tppMappingRefs.get(codeLookup);

        // return cached version if exists
        if (tppMappingRefFromCache != null) {
            return tppMappingRefFromCache;
        }

        TppMappingRef tppMappingRefFromDB = tppMappingRefDalI.getMappingFromRowId(rowId);
        if (tppMappingRefFromDB == null) {
            TransformWarnings.log(LOG, this, "Failed to find TPP mapping for {}", rowId);
            return null;
        }

        // Add to the cache
        tppMappingRefs.put(codeLookup, tppMappingRefFromDB);

        return tppMappingRefFromDB;
    }

    // Lookup code reference from SRConfigureListOption generated db
    public TppConfigListOption lookUpTppConfigListOption(CsvCell cell, AbstractCsvParser parser) throws Exception {

        Long rowId = cell.getLong();
        if (rowId < 0) {
            return null;
        }
        String codeLookup = rowId.toString() + "|" + serviceId.toString();

        //Find the code in the cache
        TppConfigListOption tppConfigListOptionFromCache = tppConfigListOptions.get(codeLookup);

        // return cached version if exists
        if (tppConfigListOptionFromCache != null) {
            return tppConfigListOptionFromCache;
        }

        TppConfigListOption tppConfigListOptionFromDB = tppConfigListOptionDalI.getListOptionFromRowId(rowId, serviceId);
        if (tppConfigListOptionFromDB == null) {
            TransformWarnings.log(LOG, parser, "TPP ConfigListOption not found for id: {},  in file: {}, line: {}",
                    rowId, parser.getFilePath(), parser.getCurrentLineNumber());

            return null;
        }

        // Add to the cache
        tppConfigListOptions.put(codeLookup, tppConfigListOptionFromDB);

        return tppConfigListOptionFromDB;
    }

    // Lookup code reference from SRImmunisationContent generated db
    public TppImmunisationContent lookUpTppImmunisationContent(Long rowId, AbstractCsvParser parser) throws Exception {

        String codeLookup = rowId.toString() + "|" + serviceId.toString();

        //Find the code in the cache
        TppImmunisationContent tppImmunisationContentFromCache = tppImmunisationContents.get(codeLookup);

        // return cached version if exists
        if (tppImmunisationContentFromCache != null) {
            return tppImmunisationContentFromCache;
        }

        TppImmunisationContent tppImmunisationContentFromDB = tppImmunisationContentDalI.getContentFromRowId(rowId);
        if (tppImmunisationContentFromDB == null) {
            TransformWarnings.log(LOG, parser, "TPP Immunisation content lookup failed for id: {},  in file: {}, line: {}",
                    rowId, parser.getFilePath(), parser.getCurrentLineNumber());

            return null;
        }

        // Add to the cache
        tppImmunisationContents.put(codeLookup, tppImmunisationContentFromDB);

        return tppImmunisationContentFromDB;
    }

    // Lookup code reference from SRCtv3Transformer generated db
    public TppCtv3Lookup lookUpTppCtv3Code(String ctv3Code, AbstractCsvParser parser) throws Exception {

        String codeLookup = ctv3Code;

        //Find the code in the cache
        TppCtv3Lookup tppCtv3LookupFromCache = tppCtv3Lookups.get(codeLookup);

        // return cached version if exists
        if (tppCtv3LookupFromCache != null) {
            return tppCtv3LookupFromCache;
        }

        TppCtv3Lookup tppCtv3LookupFromDB = tppCtv3LookupRefDal.getContentFromCtv3Code(ctv3Code);
        if (tppCtv3LookupFromDB == null) {
            TransformWarnings.log(LOG, parser, "TPP Ctv3 lookup failed for code: {},  in file: {}, line: {}",
                    ctv3Code, parser.getFilePath(), parser.getCurrentLineNumber());

            return null;
        }

        // Add to the cache
        tppCtv3Lookups.put(codeLookup, tppCtv3LookupFromDB);

        return tppCtv3LookupFromDB;
    }

    // Lookup multi-lex read code map
    public TppMultiLexToCtv3Map lookUpMultiLexToCTV3Map(Long multiLexProductId, AbstractCsvParser parser) throws Exception {

        String codeLookup = multiLexProductId.toString();

        //Find the code in the cache
        TppMultiLexToCtv3Map multiLexToCTV3MapFromCache = multiLexToCTV3Map.get(codeLookup);

        // return cached version if exists
        if (multiLexToCTV3MapFromCache != null) {
            return multiLexToCTV3MapFromCache;
        }

        TppMultiLexToCtv3Map multiLexToCTV3MapFromDB = multiLexToCTV3MapDalI.getMultiLexToCTV3Map(multiLexProductId);
        if (multiLexToCTV3MapFromDB == null) {

            TransformWarnings.log(LOG, parser, "TPP Multilex lookup failed for id: {},  in file: {}, line: {}",
                    multiLexProductId, parser.getFilePath(), parser.getCurrentLineNumber());
            return null;
        }

        // Add to the cache
        multiLexToCTV3Map.put(codeLookup, multiLexToCTV3MapFromDB);

        return multiLexToCTV3MapFromDB;
    }

    public void saveInternalId(String idType, String sourceId, String destinationId) throws Exception {
        InternalIdMap mapping = new InternalIdMap();
        mapping.setServiceId(serviceId);
        mapping.setIdType(idType);
        mapping.setSourceId(sourceId);
        mapping.setDestinationId(destinationId);

        List<InternalIdMap> list = new ArrayList<>();
        list.add(mapping);

        saveInternalIds(list);
    }

    public void saveInternalIds(List<InternalIdMap> mappings) throws Exception {

        internalIdDal.save(mappings);

        for (InternalIdMap mapping : mappings) {
            String cacheKey = mapping.getIdType() + "|" + mapping.getSourceId();
            internalIdMapCache.put(cacheKey, mapping.getDestinationId());
        }
    }

    public String getInternalId(String idType, String sourceId) throws Exception {
        String cacheKey = idType + "|" + sourceId;
        if (internalIdMapCache.containsKey(cacheKey)) {
            return internalIdMapCache.get(cacheKey);
        }

        String ret = internalIdDal.getDestinationId(serviceId, idType, sourceId);

        if (ret != null) {
            internalIdMapCache.put(cacheKey, ret);
        }

        return ret;
    }

    @Override
    public UUID getServiceId() {
        return serviceId;
    }

    @Override
    public UUID getSystemId() {
        return systemId;
    }

    @Override
    public UUID getExchangeId() {
        return exchangeId;
    }


    public String tppRelationtoFhir(String tppTerm) {
        // I considered an enum but e.g. father will include step, foster, grand etc fathers
        // Notes: contains instead of equals for fiance in case they include fiancee later
        // contains(GRAND) as the only case where a xxxFATHER is not a parent
        // Return empty string where no applicable FHIR type
        String upper = tppTerm.toUpperCase();
        if (upper.contains("CHILD") || upper.contains("BROTHER") ||
                upper.contains("SISTER") || upper.equals("UNCLE") || upper.equals("AUNT") ||
                upper.equals("FAMILY MEMBER") || upper.contains("FIANCE") ||
                upper.contains("HUSBAND") || upper.contains("WIFE") || upper.equals("NEPHEW") ||
                upper.equals("NIECE") || upper.equals("SIBLING") || upper.contains("GRAND")) {
            return "family";
        } else if (upper.contains("FATHER") || upper.contains("MOTHER")) {
            return "parent";
        } else if (upper.equals("GUARDIAN")) {
            return "guardian";
        } else if (upper.contains("PARTNER")) {
            return "partner";
        } else if (upper.equals("FRIEND") || upper.contains("FLATMATE")) {
            return "friend";
        } else if (upper.equals("POWER OF ATTORNEY") || upper.equals("SOLICITOR") || upper.contains("PROXY")) {
            return "agent";
        } else if (upper.equals("OTHER") || upper.equals("UNKNOWN") || upper.equals("NONE")) {
            return "";
        } else if (upper.equals("LANDLORD") || upper.equals("NEIGHBOUR")) {
            return "";
        } else {
            return "caregiver";
        }
    }


    private void buildKnownEthnicCodes() {

        knownEthnicCodes.put("9S1..", EthnicCategory.WHITE_BRITISH);
        knownEthnicCodes.put("XactH", EthnicCategory.WHITE_BRITISH);
        knownEthnicCodes.put("Xacus", EthnicCategory.WHITE_BRITISH);
        knownEthnicCodes.put("Xacut", EthnicCategory.WHITE_BRITISH);
        knownEthnicCodes.put("XaFwD", EthnicCategory.WHITE_BRITISH);
        knownEthnicCodes.put("XaIuh", EthnicCategory.WHITE_BRITISH);
        knownEthnicCodes.put("XaIui", EthnicCategory.WHITE_BRITISH);
        knownEthnicCodes.put("XaJRC", EthnicCategory.WHITE_BRITISH);
        knownEthnicCodes.put("XaJRD", EthnicCategory.WHITE_BRITISH);
        knownEthnicCodes.put("XaJRE", EthnicCategory.WHITE_BRITISH);
        knownEthnicCodes.put("XaJRG", EthnicCategory.WHITE_BRITISH);
        knownEthnicCodes.put("XaQEa", EthnicCategory.WHITE_BRITISH);
        knownEthnicCodes.put("9SA9.", EthnicCategory.WHITE_IRISH);
        knownEthnicCodes.put("XactI", EthnicCategory.WHITE_IRISH);
        knownEthnicCodes.put("XacuQ", EthnicCategory.WHITE_IRISH);
        knownEthnicCodes.put("XacuR", EthnicCategory.WHITE_IRISH);
        knownEthnicCodes.put("Xacuu", EthnicCategory.WHITE_IRISH);
        knownEthnicCodes.put("Xacuv", EthnicCategory.WHITE_IRISH);
        knownEthnicCodes.put("XaFwE", EthnicCategory.WHITE_IRISH);
        knownEthnicCodes.put("XaFx2", EthnicCategory.WHITE_IRISH);
        knownEthnicCodes.put("XaJQw", EthnicCategory.WHITE_IRISH);
        knownEthnicCodes.put("XaJRF", EthnicCategory.WHITE_IRISH);
        knownEthnicCodes.put("XaQEb", EthnicCategory.WHITE_IRISH);
        knownEthnicCodes.put("9SAA.", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("9SAB.", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("9SAC.", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("9T11.", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("9T12.", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XactJ", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XactK", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("Xacux", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("Xacuy", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaedN", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaedQ", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaedS", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaedT", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaedU", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaedV", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaedW", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaFwF", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaJQx", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaJRg", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaJRh", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaJRi", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaJRj", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaJRk", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaJRl", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaJRm", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaJSB", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaJSC", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaJSD", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaJSE", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaJSF", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaJSG", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaJSH", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaJSI", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaJSJ", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaJSK", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaJSL", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaJSM", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaJSN", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaJSO", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaJSP", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaJSQ", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaR4o", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaR4p", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaR61", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaVw5", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaW8w", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XaW95", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XE2Nz", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XE2O0", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XM1SF", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XM1SG", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XM1SH", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XM1SI", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("XS7AU", EthnicCategory.OTHER_WHITE);
        knownEthnicCodes.put("9S2..", EthnicCategory.MIXED_CARIBBEAN);
        knownEthnicCodes.put("XaBz7", EthnicCategory.MIXED_CARIBBEAN);
        knownEthnicCodes.put("XaBz8", EthnicCategory.MIXED_CARIBBEAN);
        knownEthnicCodes.put("XactL", EthnicCategory.MIXED_CARIBBEAN);
        knownEthnicCodes.put("XacuS", EthnicCategory.MIXED_CARIBBEAN);
        knownEthnicCodes.put("XaIB5", EthnicCategory.MIXED_CARIBBEAN);
        knownEthnicCodes.put("XaJQy", EthnicCategory.MIXED_CARIBBEAN);
        knownEthnicCodes.put("XaJRA", EthnicCategory.MIXED_CARIBBEAN);
        knownEthnicCodes.put("9S3..", EthnicCategory.MIXED_AFRICAN);
        knownEthnicCodes.put("Xactd", EthnicCategory.MIXED_AFRICAN);
        knownEthnicCodes.put("XacuT", EthnicCategory.MIXED_AFRICAN);
        knownEthnicCodes.put("XaIB6", EthnicCategory.MIXED_AFRICAN);
        knownEthnicCodes.put("XaJQz", EthnicCategory.MIXED_AFRICAN);
        knownEthnicCodes.put("Xacte", EthnicCategory.MIXED_ASIAN);
        knownEthnicCodes.put("XacuU", EthnicCategory.MIXED_ASIAN);
        knownEthnicCodes.put("Xacv0", EthnicCategory.MIXED_ASIAN);
        knownEthnicCodes.put("Xacv2", EthnicCategory.MIXED_ASIAN);
        knownEthnicCodes.put("XaJR0", EthnicCategory.MIXED_ASIAN);
        knownEthnicCodes.put("9S5..", EthnicCategory.OTHER_MIXED);
        knownEthnicCodes.put("9S51.", EthnicCategory.OTHER_MIXED);
        knownEthnicCodes.put("9SB..", EthnicCategory.OTHER_MIXED);
        knownEthnicCodes.put("9SB1.", EthnicCategory.OTHER_MIXED);
        knownEthnicCodes.put("9SB2.", EthnicCategory.OTHER_MIXED);
        knownEthnicCodes.put("9SB3.", EthnicCategory.OTHER_MIXED);
        knownEthnicCodes.put("9SB4.", EthnicCategory.OTHER_MIXED);
        knownEthnicCodes.put("Xactf", EthnicCategory.OTHER_MIXED);
        knownEthnicCodes.put("Xacua", EthnicCategory.OTHER_MIXED);
        knownEthnicCodes.put("Xacuz", EthnicCategory.OTHER_MIXED);
        knownEthnicCodes.put("XaFwG", EthnicCategory.OTHER_MIXED);
        knownEthnicCodes.put("XaJR1", EthnicCategory.OTHER_MIXED);
        knownEthnicCodes.put("XaJRH", EthnicCategory.OTHER_MIXED);
        knownEthnicCodes.put("XaJRI", EthnicCategory.OTHER_MIXED);
        knownEthnicCodes.put("XaJRJ", EthnicCategory.OTHER_MIXED);
        knownEthnicCodes.put("XaJRK", EthnicCategory.OTHER_MIXED);
        knownEthnicCodes.put("XaJRL", EthnicCategory.OTHER_MIXED);
        knownEthnicCodes.put("XaJRM", EthnicCategory.OTHER_MIXED);
        knownEthnicCodes.put("XM1S7", EthnicCategory.OTHER_MIXED);
        knownEthnicCodes.put("9S6..", EthnicCategory.ASIAN_INDIAN);
        knownEthnicCodes.put("Xactg", EthnicCategory.ASIAN_INDIAN);
        knownEthnicCodes.put("Xacuc", EthnicCategory.ASIAN_INDIAN);
        knownEthnicCodes.put("XaJR2", EthnicCategory.ASIAN_INDIAN);
        knownEthnicCodes.put("XaJRO", EthnicCategory.ASIAN_INDIAN);
        knownEthnicCodes.put("XaJRP", EthnicCategory.ASIAN_INDIAN);
        knownEthnicCodes.put("9S7..", EthnicCategory.ASIAN_PAKISTANI);
        knownEthnicCodes.put("Xacth", EthnicCategory.ASIAN_PAKISTANI);
        knownEthnicCodes.put("Xacui", EthnicCategory.ASIAN_PAKISTANI);
        knownEthnicCodes.put("XaJR3", EthnicCategory.ASIAN_PAKISTANI);
        knownEthnicCodes.put("9S8..", EthnicCategory.ASIAN_BANGLADESHI);
        knownEthnicCodes.put("Xacti", EthnicCategory.ASIAN_BANGLADESHI);
        knownEthnicCodes.put("Xacuj", EthnicCategory.ASIAN_BANGLADESHI);
        knownEthnicCodes.put("Xacv5", EthnicCategory.ASIAN_BANGLADESHI);
        knownEthnicCodes.put("XaJR4", EthnicCategory.ASIAN_BANGLADESHI);
        knownEthnicCodes.put("9SA6.", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("9SA7.", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("9SA8.", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("9T13.", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("9T14.", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("9T15.", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("9T16.", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("9T17.", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("9T18.", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("9T19.", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("9T1A.", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("9T1B.", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("9T1E.", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("Xactk", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("Xacul", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("XacvG", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("XaE4A", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("XaFwz", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("XaFx0", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("XaJR5", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("XaJRc", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("XaJRd", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("XaJRe", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("XaJRf", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("XaJRN", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("XaJRQ", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("XaJRR", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("XaJRS", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("XaJRT", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("XaJRU", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("XaJRV", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("XaJRW", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("XaJSb", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("XaJSU", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("XaJSV", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("XaJSW", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("XaJSX", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("XaJSY", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("XM1SD", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("XM1SE", EthnicCategory.OTHER_ASIAN);
        knownEthnicCodes.put("9S42.", EthnicCategory.BLACK_CARIBBEAN);
        knownEthnicCodes.put("9SA3.", EthnicCategory.BLACK_CARIBBEAN);
        knownEthnicCodes.put("Xactm", EthnicCategory.BLACK_CARIBBEAN);
        knownEthnicCodes.put("Xacun", EthnicCategory.BLACK_CARIBBEAN);
        knownEthnicCodes.put("Xacva", EthnicCategory.BLACK_CARIBBEAN);
        knownEthnicCodes.put("XacvJ", EthnicCategory.BLACK_CARIBBEAN);
        knownEthnicCodes.put("XaJR6", EthnicCategory.BLACK_CARIBBEAN);
        knownEthnicCodes.put("XE2Nt", EthnicCategory.BLACK_CARIBBEAN);
        knownEthnicCodes.put("XM1S8", EthnicCategory.BLACK_CARIBBEAN);
        knownEthnicCodes.put("XM1S9", EthnicCategory.BLACK_CARIBBEAN);
        knownEthnicCodes.put("XM1SA", EthnicCategory.BLACK_CARIBBEAN);
        knownEthnicCodes.put("9S44.", EthnicCategory.BLACK_AFRICAN);
        knownEthnicCodes.put("9SA5.", EthnicCategory.BLACK_AFRICAN);
        knownEthnicCodes.put("Xacum", EthnicCategory.BLACK_AFRICAN);
        knownEthnicCodes.put("XacvH", EthnicCategory.BLACK_AFRICAN);
        knownEthnicCodes.put("XacvI", EthnicCategory.BLACK_AFRICAN);
        knownEthnicCodes.put("XaJR7", EthnicCategory.BLACK_AFRICAN);
        knownEthnicCodes.put("XaJRX", EthnicCategory.BLACK_AFRICAN);
        knownEthnicCodes.put("XaJRZ", EthnicCategory.BLACK_AFRICAN);
        knownEthnicCodes.put("XaJST", EthnicCategory.BLACK_AFRICAN);
        knownEthnicCodes.put("XE2Nu", EthnicCategory.BLACK_AFRICAN);
        knownEthnicCodes.put("XE2Nw", EthnicCategory.BLACK_AFRICAN);
        knownEthnicCodes.put("XM1S3", EthnicCategory.BLACK_AFRICAN);
        knownEthnicCodes.put("XM1S4", EthnicCategory.BLACK_AFRICAN);
        knownEthnicCodes.put("9S4..", EthnicCategory.OTHER_BLACK);
        knownEthnicCodes.put("9S41.", EthnicCategory.OTHER_BLACK);
        knownEthnicCodes.put("9S43.", EthnicCategory.OTHER_BLACK);
        knownEthnicCodes.put("9S45.", EthnicCategory.OTHER_BLACK);
        knownEthnicCodes.put("9S46.", EthnicCategory.OTHER_BLACK);
        knownEthnicCodes.put("9S47.", EthnicCategory.OTHER_BLACK);
        knownEthnicCodes.put("9S48.", EthnicCategory.OTHER_BLACK);
        knownEthnicCodes.put("Xactl", EthnicCategory.OTHER_BLACK);
        knownEthnicCodes.put("Xactn", EthnicCategory.OTHER_BLACK);
        knownEthnicCodes.put("Xacuo", EthnicCategory.OTHER_BLACK);
        knownEthnicCodes.put("XacvZ", EthnicCategory.OTHER_BLACK);
        knownEthnicCodes.put("XaFwH", EthnicCategory.OTHER_BLACK);
        knownEthnicCodes.put("XaFwy", EthnicCategory.OTHER_BLACK);
        knownEthnicCodes.put("XaJR8", EthnicCategory.OTHER_BLACK);
        knownEthnicCodes.put("XaJRa", EthnicCategory.OTHER_BLACK);
        knownEthnicCodes.put("XaJRb", EthnicCategory.OTHER_BLACK);
        knownEthnicCodes.put("XaJRY", EthnicCategory.OTHER_BLACK);
        knownEthnicCodes.put("XE2Nx", EthnicCategory.OTHER_BLACK);
        knownEthnicCodes.put("XE2Ny", EthnicCategory.OTHER_BLACK);
        knownEthnicCodes.put("XM1S5", EthnicCategory.OTHER_BLACK);
        knownEthnicCodes.put("XM1S6", EthnicCategory.OTHER_BLACK);
        knownEthnicCodes.put("9T1C.", EthnicCategory.CHINESE);
        knownEthnicCodes.put("9T1C.", EthnicCategory.CHINESE);
        knownEthnicCodes.put("Xactj", EthnicCategory.CHINESE);
        knownEthnicCodes.put("Xacuk", EthnicCategory.CHINESE);
        knownEthnicCodes.put("XacvF", EthnicCategory.CHINESE);
        knownEthnicCodes.put("XaJR9", EthnicCategory.OTHER);
        knownEthnicCodes.put("9S52.", EthnicCategory.OTHER);
        knownEthnicCodes.put("9SA..", EthnicCategory.OTHER);
        knownEthnicCodes.put("9SA1.", EthnicCategory.OTHER);
        knownEthnicCodes.put("9SA4.", EthnicCategory.OTHER);
        knownEthnicCodes.put("9T1..", EthnicCategory.OTHER);
        knownEthnicCodes.put("9T1Y.", EthnicCategory.OTHER);
        knownEthnicCodes.put("Xacto", EthnicCategory.OTHER);
        knownEthnicCodes.put("Xactp", EthnicCategory.OTHER);
        knownEthnicCodes.put("Xacup", EthnicCategory.OTHER);
        knownEthnicCodes.put("Xacuq", EthnicCategory.OTHER);
        knownEthnicCodes.put("Xacvb", EthnicCategory.OTHER);
        knownEthnicCodes.put("Xacvc", EthnicCategory.OTHER);
        knownEthnicCodes.put("XaFx1", EthnicCategory.OTHER);
        knownEthnicCodes.put("XaJSa", EthnicCategory.OTHER);
        knownEthnicCodes.put("XaJSg", EthnicCategory.OTHER);
        knownEthnicCodes.put("XaJSR", EthnicCategory.OTHER);
        knownEthnicCodes.put("XaJSS", EthnicCategory.OTHER);
        knownEthnicCodes.put("XaJSZ", EthnicCategory.OTHER);
        knownEthnicCodes.put("XaN9x", EthnicCategory.OTHER);
        knownEthnicCodes.put("XM1SB", EthnicCategory.OTHER);
        knownEthnicCodes.put("XM1SC", EthnicCategory.OTHER);
        knownEthnicCodes.put("9SA2.", EthnicCategory.NOT_STATED);
        knownEthnicCodes.put("9SZ..", EthnicCategory.NOT_STATED);
        knownEthnicCodes.put("9T1Z.", EthnicCategory.NOT_STATED);
        knownEthnicCodes.put("XaJRB", EthnicCategory.NOT_STATED);
        knownEthnicCodes.put("XaJSc", EthnicCategory.NOT_STATED);
        knownEthnicCodes.put("XaJSd", EthnicCategory.NOT_STATED);
        knownEthnicCodes.put("XaJSe", EthnicCategory.NOT_STATED);
        knownEthnicCodes.put("XaJSf", EthnicCategory.NOT_STATED);
        knownEthnicCodes.put("Y1527", EthnicCategory.NOT_STATED);
        knownEthnicCodes.put("Y16b7", EthnicCategory.NOT_STATED);
    }

    public StaffMemberCache getStaffMemberCache() {
        return staffMemberCache;
    }

    public AppointmentFlagCache getAppointmentFlagCache() {
        return appointmentFlagCache;
    }

    public PatientResourceCache getPatientResourceCache() {
        return patientResourceCache;
    }

    public ReferralRequestResourceCache getReferralRequestResourceCache() {
        return referralRequestResourceCache;
    }

    public ConditionResourceCache getConditionResourceCache() {
        return conditionResourceCache;
    }

    public void submitToThreadPool(Callable callable) throws Exception {
        if (this.utilityThreadPool == null) {
            int threadPoolSize = ConnectionManager.getPublisherTransformConnectionPoolMaxSize(serviceId);
            this.utilityThreadPool = new ThreadPool(threadPoolSize, 50000);
        }

        List<ThreadPoolError> errors = utilityThreadPool.submit(callable);
        AbstractCsvCallable.handleErrors(errors);
    }

    public void waitUntilThreadPoolIsEmpty() throws Exception {
        if (this.utilityThreadPool != null) {
            List<ThreadPoolError> errors = utilityThreadPool.waitUntilEmpty();
            AbstractCsvCallable.handleErrors(errors);
        }
    }

    public void stopThreadPool() throws Exception {
        if (this.utilityThreadPool != null) {
            List<ThreadPoolError> errors = utilityThreadPool.waitAndStop();
            AbstractCsvCallable.handleErrors(errors);
        }
    }

//    public boolean isProcedure(String ctv3Code) throws Exception {
//
//        if (codeToTypes.get(ctv3Code).equals(ResourceType.Procedure)) {
//            return true;
//        }
//        if (ctv3HierarchyRefDalI.isChildCodeUnderParentCode(ctv3Code, OPERATIONS_PROCEDURES)) {
//            codeToTypes.put(ctv3Code, ResourceType.Procedure);
//            return true;
//        }
//        return false;
//    }
//
//    public boolean isDisorder(String ctv3Code) throws Exception {
//        if (codeToTypes.get(ctv3Code).equals(ResourceType.Condition)) {
//            return true;
//        }
//        if (ctv3HierarchyRefDalI.isChildCodeUnderParentCode(ctv3Code, DISORDERS)) {
//            codeToTypes.put(ctv3Code, ResourceType.Condition);
//            return true;
//        }
//        return false;
//    }
//
//    public boolean isFamilyHistoryDisorder(String ctv3Code) throws Exception {
//        if (codeToTypes.get(ctv3Code).equals(ResourceType.FamilyMemberHistory)) {
//            return true;
//        }
//        if (ctv3HierarchyRefDalI.isChildCodeUnderParentCode(ctv3Code, FAMILY_HISTORY_DISORDERS)) {
//            codeToTypes.put(ctv3Code, ResourceType.FamilyMemberHistory);
//            return true;
//        }
//        return false;
//    }

    public ResourceType getResourceType(String code) throws Exception {
        if (codeToTypes.containsKey(code)) {
            return codeToTypes.get(code);
        } else {
            if (ctv3HierarchyRefDalI.isChildCodeUnderParentCode(code, OPERATIONS_PROCEDURES)) {
                codeToTypes.put(code, ResourceType.Procedure);
                return ResourceType.Procedure;
            } else if (ctv3HierarchyRefDalI.isChildCodeUnderParentCode(code, FAMILY_HISTORY_DISORDERS)) {
                codeToTypes.put(code, ResourceType.FamilyMemberHistory);
                return ResourceType.FamilyMemberHistory;
            } else if (ctv3HierarchyRefDalI.isChildCodeUnderParentCode(code, DISORDERS)) {
                codeToTypes.put(code, ResourceType.Condition);
                return ResourceType.Condition;
            } else if (ctv3HierarchyRefDalI.isChildCodeUnderParentCode(code, ALLERGIC_DISORDER)) {
                codeToTypes.put(code, ResourceType.AllergyIntolerance);
             return ResourceType.AllergyIntolerance;
            }

        }
        return null;

    }
}