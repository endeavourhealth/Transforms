package org.endeavourhealth.transform.tpp.csv.helpers;

import com.google.common.base.Strings;
import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherCommon.*;
import org.endeavourhealth.core.database.dal.publisherCommon.models.*;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.referenceLists.ReferenceList;
import org.endeavourhealth.transform.common.referenceLists.ReferenceListNoCsvCells;
import org.endeavourhealth.transform.common.referenceLists.ReferenceListSingleCsvCells;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ResourceBuilderBase;
import org.endeavourhealth.transform.tpp.csv.helpers.cache.*;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import static org.endeavourhealth.core.terminology.Read2.isBPCode;

public class TppCsvHelper implements HasServiceSystemAndExchangeIdI {
    private static final Logger LOG = LoggerFactory.getLogger(TppCsvHelper.class);

    private static final String OPERATIONS_PROCEDURES = "X0001";
    private static final String DISORDERS = "X0003";
    private static final String FAMILY_HISTORY_DISORDERS = "Xa1px";
    private static final String ALLERGIC_DISORDER = "Xa1pQ";
    private static final ParserPool PARSER_POOL = new ParserPool();

    private static TppMappingRefDalI tppMappingRefDalI = DalProvider.factoryTppMappingRefDal();
    private static TppConfigListOptionDalI tppConfigListOptionDalI = DalProvider.factoryTppConfigListOptionDal();
    private static TppImmunisationContentDalI tppImmunisationContentDalI = DalProvider.factoryTppImmunisationContentDal();
    private static InternalIdDalI internalIdDal = DalProvider.factoryInternalIdDal();
    private static TppMultilexLookupDalI multiLexToCTV3MapDalI = DalProvider.factoryTppMultiLexDal();
    private static TppCtv3HierarchyRefDalI ctv3HierarchyRefDalI = DalProvider.factoryTppCtv3HierarchyRefDal();
    private static ResourceDalI resourceRepository = DalProvider.factoryResourceDal();

    //note the below are static maps so they will apply to all transforms until the app is restarted
    private static Map<Integer, TppMappingRef> hmTppMappingRefs = new ConcurrentHashMap<>();
    private static Map<Integer, TppConfigListOption> hmTppConfigListOptions = new ConcurrentHashMap<>();
    private static Map<Integer, TppImmunisationContent> hmTppImmunisationContents = new ConcurrentHashMap<>();
    private static Map<Integer, TppMultilexProductToCtv3Map> hmMultiLexProductIdToCTV3Map = new ConcurrentHashMap<>();
    private static Map<Integer, String> hmMultilexActionGroupNames = new ConcurrentHashMap<>();
    private static Map<StringMemorySaver, StringMemorySaver> hmInternalIdMapCache = new ConcurrentHashMap<>();

    private Map<Long, ReferenceList> consultationNewChildMap = new ConcurrentHashMap<>();
    private Map<Long, ReferenceList> consultationExistingChildMap = new ConcurrentHashMap<>();
    private Map<Long, ReferenceList> encounterAppointmentOrVisitMap = new ConcurrentHashMap<>();
    private TppRecordStatusCache recordStatusHelper = new TppRecordStatusCache();
    private TppReferralStatusCache referralStatusCache = new TppReferralStatusCache();
    private StaffMemberCache staffMemberCache = new StaffMemberCache();
    private AppointmentFlagCache appointmentFlagCache = new AppointmentFlagCache();
    private PatientResourceCache patientResourceCache = new PatientResourceCache();
    private ConditionResourceCache conditionResourceCache = new ConditionResourceCache();
    private RotaDetailsCache rotaDateAndStaffCache = new RotaDetailsCache();
    private Map<Long, DateAndCode> ethnicityMap = new HashMap<>();
    private Map<Long, DateAndCode> maritalStatusMap = new HashMap<>();
    private ThreadPool utilityThreadPool = null;
    private Map<String, ResourceType> codeToTypes = new HashMap<>();


    private final UUID serviceId;
    private final UUID systemId;
    private final UUID exchangeId;

    public TppCsvHelper(UUID serviceId, UUID systemId, UUID exchangeId) {
        this.serviceId = serviceId;
        this.systemId = systemId;
        this.exchangeId = exchangeId;
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

    public Reference createPractitionerReferenceForProfileId(CsvCell profileIdCell) throws Exception {
        Object profileId = getStaffMemberCache().findProfileId(serviceId, profileIdCell);
        if (profileId == null) {
            return null;
        }
        return ReferenceHelper.createReference(ResourceType.Practitioner, profileId.toString());
    }

    public Reference createPractitionerReferenceForStaffMemberId(CsvCell staffMemberIdCell, CsvCell organisationDoneAtCell) throws Exception {
        Object profileId = getStaffMemberCache().findProfileIdForStaffMemberAndOrg(serviceId, staffMemberIdCell, organisationDoneAtCell);
        if (profileId == null) {
            return null;
        }
        return ReferenceHelper.createReference(ResourceType.Practitioner, profileId.toString());
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

    public void cacheNewEncounterAppointmentLink(CsvCell eventIdCell,
                                                 String resourceGuid,
                                                 ResourceType resourceType) {

        Long eventId = eventIdCell.getLong();
        ReferenceList list = encounterAppointmentOrVisitMap.get(eventId);
        if (list == null) {
            //we know there will only be a single CsvCell so use this implementation to save memory
            list = new ReferenceListSingleCsvCells();
            //list = new ReferenceList();
            encounterAppointmentOrVisitMap.put(eventId, list);
        }

        String resourceLocalUniqueId = resourceGuid;
        Reference resourceReference = ReferenceHelper.createReference(resourceType, resourceLocalUniqueId);
        list.add(resourceReference, eventIdCell);
    }

    public ReferenceList getAndRemoveEncounterAppointmentLink(CsvCell eventIdCell) {

        Long eventId = eventIdCell.getLong();
        return encounterAppointmentOrVisitMap.remove(eventId);
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


    public TppRecordStatusCache getRecordStatusHelper() {
        return recordStatusHelper;
    }

    public TppReferralStatusCache getReferralStatusCache() {
        return referralStatusCache;
    }

    public void cacheEthnicity(CsvCell patientGuid, DateTimeType fhirDate, EthnicCategory ethnicCategory, CsvCell srCodeIdCell) {

        //we may have a null ethnic category, if we know a code is an ethnicity one, but isn't one that can be mapped
        String code = null;
        if (ethnicCategory != null) {
            code = ethnicCategory.getCode();
        }

        DateAndCode dc = ethnicityMap.get(patientGuid.getLong());
        if (dc == null
                || dc.isBefore(fhirDate)) {
            ethnicityMap.put(patientGuid.getLong(), new DateAndCode(fhirDate, code, srCodeIdCell));
        }
    }

    public DateAndCode findEthnicity(CsvCell patientGuid) {
        return ethnicityMap.remove(patientGuid.getLong());
    }

    public void cacheMaritalStatus(CsvCell patientGuid, DateTimeType fhirDate, MaritalStatus maritalStatus, CsvCell srCodeIdCell) {

        //we may have a null ethnic category, if we know a code is a marital status ones, but isn't one that can be mapped
        String code = null;
        if (maritalStatus != null) {
            code = maritalStatus.getCode();
        }

        DateAndCode dc = maritalStatusMap.get(patientGuid.getLong());
        if (dc == null
                || dc.isBefore(fhirDate)) {
            maritalStatusMap.put(patientGuid.getLong(), new DateAndCode(fhirDate, code, srCodeIdCell));
        }
    }

    public DateAndCode findMaritalStatus(CsvCell patientGuid) {
        return maritalStatusMap.remove(patientGuid.getLong());
    }

    /**
     * Lookup code reference from SRMapping generated db
     */
    public TppMappingRef lookUpTppMappingRef(CsvCell cell) throws Exception {

        if (isEmptyOrNegative(cell)) {
            return null;
        }

        Integer rowId = cell.getInt();

        //Find the code in the cache
        TppMappingRef ret = hmTppMappingRefs.get(rowId);
        if (ret == null) {
            ret = tppMappingRefDalI.getMappingFromRowId(rowId.intValue());
            if (ret == null) {
                TransformWarnings.log(LOG, this, "Failed to find TPP mapping for {}", cell);
                return null;
            }

            hmTppMappingRefs.put(rowId, ret);
        }

        return ret;
    }

    /**
     * Lookup code reference from SRConfigureListOption generated db
     */
    public static TppConfigListOption lookUpTppConfigListOption(CsvCell cell) throws Exception {

        if (isEmptyOrNegative(cell)) {
            return null;
        }

        //Find the code in the cache
        Integer rowId = cell.getInt();
        TppConfigListOption ret = hmTppConfigListOptions.get(rowId);
        if (ret == null) {
            ret = tppConfigListOptionDalI.getListOptionFromRowId(rowId.intValue());

            //we've done about 100 TPP practices and haven't had any unexplained missing records, so
            //now treat this as a hard fail rather than something to pick up after the fact
            if (ret == null) {
                throw new Exception("Failed to find Configured List option for ID " + rowId);
            }

            hmTppConfigListOptions.put(rowId, ret);
        }

        return ret;
    }

    /**
     * Lookup code reference from SRImmunisationContent generated db
     */
    public static TppImmunisationContent lookUpTppImmunisationContent(CsvCell immContentCell) throws Exception {

        if (immContentCell.isEmpty()
                || immContentCell.getLong().longValue() < 0) {
            return null;
        }

        Integer rowId = immContentCell.getInt();

        TppImmunisationContent ret = hmTppImmunisationContents.get(rowId);
        if (ret == null) {
            ret = tppImmunisationContentDalI.getContentFromRowId(rowId.intValue());
            if (ret == null) {
                throw new Exception("Failed to find Immunisation Content record for ID " + rowId);
            }

            hmTppImmunisationContents.put(rowId, ret);
        }

        return ret;
    }


    /**
     * Lookup multilex read code map
     */
    public TppMultilexProductToCtv3Map lookUpMultilexToCTV3Map(CsvCell multiLexProductIdCell) throws Exception {

        if (isEmptyOrNegative(multiLexProductIdCell)) {
            return null;
        }

        Integer productId = multiLexProductIdCell.getInt();

        TppMultilexProductToCtv3Map ret = hmMultiLexProductIdToCTV3Map.get(productId);
        if (ret == null) {
            ret = multiLexToCTV3MapDalI.getMultilexToCtv3MapForProductId(productId.intValue());
            if (ret == null) {
                TransformWarnings.log(LOG, this, "TPP Multilex lookup failed for product ID {}", multiLexProductIdCell);
                return null;
            }

            // Add to the cache
            hmMultiLexProductIdToCTV3Map.put(productId, ret);
        }

        return ret;
    }

    /**
     * looks up a Multilex Action Group name for its ID
     */
    public String lookUpMultilexActionGroupNameForId(CsvCell multilexActionIdCell) throws Exception {

        if (isEmptyOrNegative(multilexActionIdCell)) {
            return null;
        }

        Integer id = multilexActionIdCell.getInt();
        String ret = hmMultilexActionGroupNames.get(id);
        if (ret == null) {
            ret = multiLexToCTV3MapDalI.getMultilexActionGroupNameForId(id.intValue());
            if (ret == null) {
                TransformWarnings.log(LOG, this, "TPP Action Group lookup failed for ID {}", multilexActionIdCell);
                return null;
            }

            hmMultilexActionGroupNames.put(id, ret);
        }

        return ret;
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

       // add them to the cache
        for (InternalIdMap mapping : mappings) {
            StringMemorySaver cacheKey = new StringMemorySaver(mapping.getIdType() + "|" + mapping.getSourceId());
            hmInternalIdMapCache.put(cacheKey, new StringMemorySaver(mapping.getDestinationId()));
        }
    }

    public void saveInternalIdsNoCache(List<InternalIdMap> mappings) throws Exception {

        internalIdDal.save(mappings);

        //add them to the cache
//        for (InternalIdMap mapping : mappings) {
//            StringMemorySaver cacheKey = new StringMemorySaver(mapping.getIdType() + "|" + mapping.getSourceId());
//            hmInternalIdMapCache.put(cacheKey, new StringMemorySaver(mapping.getDestinationId()));
//        }
    }

    public String getInternalId(String idType, String sourceId) throws Exception {
        StringMemorySaver cacheKey = new StringMemorySaver(idType + "|" + sourceId);
        StringMemorySaver cached = hmInternalIdMapCache.get(cacheKey);

        if (cached != null) {
            return cached.toString();
        }

        String ret = internalIdDal.getDestinationId(serviceId, idType, sourceId);

        //add to cache
        if (ret != null) {
            hmInternalIdMapCache.put(cacheKey, new StringMemorySaver(ret));
        }

        return ret;
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


    public StaffMemberCache getStaffMemberCache() {
        return staffMemberCache;
    }

    public AppointmentFlagCache getAppointmentFlagCache() {
        return appointmentFlagCache;
    }

    public PatientResourceCache getPatientResourceCache() {
        return patientResourceCache;
    }

    public ConditionResourceCache getConditionResourceCache() {
        return conditionResourceCache;
    }

    public RotaDetailsCache getRotaDateAndStaffCache() {
        return rotaDateAndStaffCache;
    }

    public void submitToThreadPool(Callable callable) throws Exception {
        if (this.utilityThreadPool == null) {
            int threadPoolSize = ConnectionManager.getPublisherTransformConnectionPoolMaxSize(serviceId);
            this.utilityThreadPool = new ThreadPool(threadPoolSize, 1000, "TppCsvHelper"); //lower from 50k to save memory
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
        }

        if (ctv3HierarchyRefDalI.isChildCodeUnderParentCode(code, OPERATIONS_PROCEDURES)) {
            cacheCTV3CodeToResourceType(code, ResourceType.Procedure);
            if (isBPCode(code)) {
                cacheCTV3CodeToResourceType(code, ResourceType.Observation);
                return ResourceType.Observation;
            }
            return ResourceType.Procedure;
        } else if (ctv3HierarchyRefDalI.isChildCodeUnderParentCode(code, ALLERGIC_DISORDER)) {
            cacheCTV3CodeToResourceType(code, ResourceType.AllergyIntolerance);
            return ResourceType.AllergyIntolerance;
        } else if (ctv3HierarchyRefDalI.isChildCodeUnderParentCode(code, FAMILY_HISTORY_DISORDERS)) {
            cacheCTV3CodeToResourceType(code, ResourceType.FamilyMemberHistory);
            return ResourceType.FamilyMemberHistory;
        } else if (ctv3HierarchyRefDalI.isChildCodeUnderParentCode(code, DISORDERS)) {
            cacheCTV3CodeToResourceType(code, ResourceType.Condition);
            return ResourceType.Condition;
        } else {
            cacheCTV3CodeToResourceType(code, ResourceType.Observation);
            return ResourceType.Observation;
        }
    }

    public void cacheCTV3CodeToResourceType(String code, ResourceType type) {
        codeToTypes.put(code, type);
    }


    /**
     * when the transform is complete, if there's any values left in the ethnicity and marital status maps,
     * then we need to update pre-existing patients with new data
     */
    public void processRemainingEthnicitiesAndMartialStatuses(FhirResourceFiler fhirResourceFiler) throws Exception {

        //get a combined list of the keys (patientGuids) from both maps
        HashSet<Long> patientIds = new HashSet<>(ethnicityMap.keySet());
        patientIds.addAll(new HashSet<>(maritalStatusMap.keySet()));

        for (Long patientId: patientIds) {

            DateAndCode newEthnicity = ethnicityMap.get(patientId);
            DateAndCode newMaritalStatus = maritalStatusMap.get(patientId);

            Patient fhirPatient = (Patient)retrieveResource("" + patientId, ResourceType.Patient);
            if (fhirPatient == null) {
                //if we try to update the ethnicity on a deleted patient, or one we've never received, just skip it
                continue;
            }

            PatientBuilder patientBuilder = new PatientBuilder(fhirPatient);

            TppMappingHelper.applyNewEthnicity(newEthnicity, patientBuilder);
            TppMappingHelper.applyNewMaritalStatus(newMaritalStatus, patientBuilder);

            fhirResourceFiler.savePatientResource(null, false, patientBuilder);
        }
    }

    public List<Resource> retrieveAllResourcesForPatient(CsvCell patientIdCell, FhirResourceFiler fhirResourceFiler) throws Exception {

        UUID edsPatientId = IdHelper.getEdsResourceId(fhirResourceFiler.getServiceId(), ResourceType.Patient, patientIdCell.getString());
        if (edsPatientId == null) {
            return new ArrayList<>();
        }

        List<Resource> ret = new ArrayList<>();

        List<ResourceWrapper> resourceWrappers = resourceRepository.getResourcesByPatient(getServiceId(), edsPatientId);
        for (ResourceWrapper resourceWrapper: resourceWrappers) {
            ret.add(resourceWrapper.getResource());
        }

        return ret;
    }

    /**
     * the TPP extract often contains -1 or 0 as a "null" value rather than being empty, so this fn
     * tests for both
     */
    public static boolean isEmptyOrNegative(CsvCell csvCell) {
        return csvCell.isEmpty()
                || csvCell.getLong().longValue() <= 0L;
    }


    public class DateAndCode {
        private DateTimeType date = null;
        private String code = null;
        private CsvCell[] additionalSourceCells;

        public DateAndCode(DateTimeType date, String code, CsvCell... additionalSourceCells) {
            this.date = date;
            this.code = code;
            this.additionalSourceCells = additionalSourceCells;
        }

        public DateTimeType getDate() {
            return date;
        }

        public String getCode() {
            return code;
        }

        public boolean hasCode() {
            return !Strings.isNullOrEmpty(code);
        }

        public CsvCell[] getAdditionalSourceCells() {
            return additionalSourceCells;
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
}