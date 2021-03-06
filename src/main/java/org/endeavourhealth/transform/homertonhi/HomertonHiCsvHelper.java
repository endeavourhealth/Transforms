package org.endeavourhealth.transform.homertonhi;

import com.google.common.base.Strings;
import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.admin.ServiceDalI;
import org.endeavourhealth.core.database.dal.admin.models.Service;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherTransform.CernerCodeValueRefDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.homertonhi.cache.OrganisationResourceCache;
import org.endeavourhealth.transform.homertonhi.cache.PatientResourceCache;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class HomertonHiCsvHelper implements HasServiceSystemAndExchangeIdI {
    private static final Logger LOG = LoggerFactory.getLogger(HomertonHiCsvHelper.class);

    //unsupported code oids which have been checked and handled as freetext
    public static final String CODE_TYPE_LOINC_URN = "2.16.840.1.113883.6.1";
    public static final String CODE_TYPE_CERNER_APRDRG = "urn:cerner:codingsystem:drg:aprdrg";

    public static final String CODE_TYPE_SNOMED_URN = "2.16.840.1.113883.6.96";
    public static final String CODE_TYPE_SNOMED_CT_URN = "2.16.840.1.113883.2.1.3.2.4.15";
    public static final String CODE_TYPE_FREETEXT = "freetext";
    public static final String CODE_TYPE_ICD10_CM_URN = "2.16.840.1.113883.6.90";
    public static final String CODE_TYPE_ICD10_URN = "2.16.840.1.113883.6.3";
    public static final String CODE_TYPE_PTCARE_URN = "urn:cerner:coding:codingsystem:nomenclature.source_vocab:PTCARE";
    public static final String CODE_TYPE_CERNER_URN = "urn:cerner:coding:codingsystem:nomenclature.source_vocab:CERNER";

    public static final String CODE_TYPE_OPCS4_URN = "2.16.840.1.113883.2.1.3.2.4.16.67";

    public static final String CODE_TYPE_CONDITION_PROBLEM   = "55607006";
    public static final String CODE_TYPE_CONDITION_DIAGNOSIS = "282291009";

    public static final String HASH_VALUE_TO_LOCAL_ID = "HASH_VALUE_TO_LOCAL_ID";

    public static final String ROYAL_FREE_HOSPITAL_ODS = "RAL";
    public static final String HOMERTON_UNIVERSITY_HOSPITAL_ODS = "RQX";

//    public static final String CODE_TYPE_ICD_10 = "ICD-10";
//    public static final String CODE_TYPE_OPCS_4 = "OPCS4";
//
//    private static HashMap<String, CernerCodeValueRef> cernerCodes = new HashMap<>();
//    private static HashMap<String, ResourceId> resourceIds = new HashMap<>();
//    private static HashMap<String, String> internalIdMapCache = new HashMap<>();
//
//    //non-static caches
//    private Map<Long, UUID> encounterIdToEncounterResourceMap = new HashMap<>();
//    private Map<Long, UUID> encounterIdToPatientResourceMap = new HashMap<>();
//    private Map<Long, UUID> personIdToPatientResourceMap = new HashMap<>();
//    private Map<Long, ReferenceList> clinicalEventChildMap = new HashMap<>();
//    private Map<Long, ReferenceList> consultationNewChildMap = new ConcurrentHashMap<>();
//    private Map<Long, String> encounterIdToPersonIdMap = new HashMap<>(); //specifically not a concurrent map because we don't multi-thread and add null values
//    private Map<String, CsvCell> codeValueNHSAlias = new HashMap<>();
//
    private Map<String, CernerCodeValueRef> cernerCodes = new ConcurrentHashMap<>();
    private Map<String, CsvCell> procedureComments = new ConcurrentHashMap<>();

    private PatientResourceCache patientCache = new PatientResourceCache();
//    private EncounterResourceCache encounterCache = new EncounterResourceCache();
//    private LocationResourceCache locationCache = new LocationResourceCache();
    private OrganisationResourceCache organisationCache = new OrganisationResourceCache();
//
    private InternalIdDalI internalIdDal = DalProvider.factoryInternalIdDal();
    private List<InternalIdMap> internalIdSaveBatch = new ArrayList<>();
    private ResourceDalI resourceRepository = DalProvider.factoryResourceDal();
    private CernerCodeValueRefDalI cernerCodeValueRefDal = DalProvider.factoryCernerCodeValueRefDal();
    private ServiceDalI serviceRepository = DalProvider.factoryServiceDal();

    private ThreadPool utilityThreadPool = null;

    private Map<StringMemorySaver, StringMemorySaver> internalIdMapCache = new HashMap<>(); //contains nulls so a regular map but uses the cacheLock
    private ReentrantLock cacheLock = new ReentrantLock();

    private UUID serviceId = null;
    private UUID systemId = null;
    private UUID exchangeId = null;
    private String version = null;

    private Set<String> personIdsToFilterOn = null;

    public HomertonHiCsvHelper(UUID serviceId, UUID systemId, UUID exchangeId, String version) {
        this.serviceId = serviceId;
        this.systemId = systemId;
        this.exchangeId = exchangeId;
        this.version = version;
    }


    public UUID getServiceId() {
        return serviceId;
    }

    public UUID getSystemId() {
        return systemId;
    }

    public UUID getExchangeId() {
        return exchangeId;
    }

    public String getVersion() {
        return version;
    }

    public PatientResourceCache getPatientCache() {
        return patientCache;
    }


//    public EncounterResourceCache getEncounterCache() { return encounterCache; }

//    public LocationResourceCache getLocationCache() { return locationCache; }

    public OrganisationResourceCache getOrganisationCache() { return organisationCache; }

    public Service getService (UUID id) throws Exception { return serviceRepository.getById(id);}
//
//    // if the resource is already filed and has been retrieved from the DB, the sourceId will differ from the
//    // saved (mapped) resource Id
//    public boolean isResourceIdMapped (String sourceId, DomainResource resource) {
//        return !resource.getId().equals(sourceId);
//    }

    public Reference createOrganisationReference(String organizationGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Organization, organizationGuid);
    }

//    public Reference createLocationReference(String locationGuid) throws Exception {
//        return ReferenceHelper.createReference(ResourceType.Location, locationGuid);
//    }
//
//    public Reference createPractitionerReference(String practitionerGuid) {
//        return ReferenceHelper.createReference(ResourceType.Practitioner, practitionerGuid);
//    }
//
//    public List<Resource> retrieveResourceByPatient(UUID patientId) throws Exception {
//        List<Resource> ret = null;
//        List<ResourceWrapper> resourceList = resourceRepository.getResourcesByPatient(serviceId, patientId);
//        for (ResourceWrapper rw : resourceList) {
//            if (ret == null) {
//                ret = new ArrayList<>();
//            }
//            String json = rw.getResourceData();
//            ret.add(ParserPool.getInstance().parse(json));
//        }
//        return ret;
//    }
//

    public boolean processRecordFilteringOnPatientId(AbstractCsvParser parser) throws TransformException {
        CsvCell personIdCell = parser.getCell("empi_id");
                    //if nothing that looks like a person ID, process the record
        if (personIdCell == null) {
            throw new TransformException("No empi_id column on parser " + parser.getFilePath());
        }

        String personId = personIdCell.getString();
        return processRecordFilteringOnPatientId(personId);
    }

    public boolean processRecordFilteringOnPatientId(String personId) {

        if (personIdsToFilterOn == null) {
            String filePath = TransformConfig.instance().getCernerPatientIdFile();  //use same cerner config
            if (Strings.isNullOrEmpty(filePath)) {
                LOG.debug("Not filtering on patients");
                personIdsToFilterOn = new HashSet<>();

            } else {
                personIdsToFilterOn = new HashSet<>();
                try {
                    List<String> lines = Files.readAllLines(new File(filePath).toPath());
                    for (String line : lines) {
                        line = line.trim();

                        //ignore comments
                        if (line.startsWith("#")) {
                            continue;
                        }
                        personIdsToFilterOn.add(line);
                    }
                    LOG.debug("Filtering on " + personIdsToFilterOn.size() + " patients from " + filePath);

                } catch (Exception ex) {
                    LOG.error("Error reading in person ID file " + filePath, ex);
                }
            }
        }

        //if no filtering IDs
        if (personIdsToFilterOn.isEmpty()) {
            return true;
        }

        //many files have an empty person ID when they're being deleted, and we don't want to skip processing them
        if (Strings.isNullOrEmpty(personId)) {
            return true;
        }

        return personIdsToFilterOn.contains(personId);
    }

    public void saveHashValueToLocalId(CsvCell hashValueCell, CsvCell localIdCell) throws Exception {
        String hashValue = hashValueCell.getString();
        String localId = localIdCell.getString();
        saveInternalId(HASH_VALUE_TO_LOCAL_ID, hashValue, localId);
    }

    public String findLocalIdFromHashValue(CsvCell hashValueCell) throws Exception {
        return getInternalId(HASH_VALUE_TO_LOCAL_ID, hashValueCell.getString());
    }

    public void saveInternalId(String idType, String sourceId, String destinationId) throws Exception {

        StringMemorySaver cacheKey = new StringMemorySaver(idType + "|" + sourceId);
        StringMemorySaver cacheValue = new StringMemorySaver(destinationId);

        List<InternalIdMap> batchToSave = null;

        try {
            cacheLock.lock();

            //check to see if the current value is the same, in which case just return out
            if (internalIdMapCache.containsKey(cacheKey)) {
                StringMemorySaver currentValue = internalIdMapCache.get(cacheKey);
                if (currentValue != null
                        && currentValue.equals(cacheValue)) {
                    return;
                }
            }

            //add/replace in the cache
            internalIdMapCache.put(cacheKey, cacheValue);

            //add to the queue to be saved
            InternalIdMap dbObj = new InternalIdMap();
            dbObj.setDestinationId(destinationId);
            dbObj.setSourceId(sourceId);
            dbObj.setIdType(idType);
            dbObj.setServiceId(serviceId);

            internalIdSaveBatch.add(dbObj);
            if (internalIdSaveBatch.size() > TransformConfig.instance().getResourceSaveBatchSize()) {
                batchToSave = new ArrayList<>(internalIdSaveBatch);
                internalIdSaveBatch.clear();
            }

        } finally {
            cacheLock.unlock();
        }

        if (batchToSave != null) {
            saveInternalIdBatch(batchToSave);
        }
    }

    private void saveInternalIdBatch(List<InternalIdMap> batch) throws Exception {
        if (batch.isEmpty()) {
            return;
        }
        internalIdDal.save(batch);
    }


    public String getInternalId(String idType, String sourceId) throws Exception {
        Set<String> hs = new HashSet<>();
        hs.add(sourceId);

        Map<String, String> hm = getInternalIds(idType, hs);
        return hm.get(sourceId);
    }

    public Map<String, String> getInternalIds(String idType, Set<String> sourceIds) throws Exception {

        Map<String, String> ret = new HashMap<>();

        //check the cache - note we cache null lookups in the cache
        Set<String> idsForDb = new HashSet<>();
        for (String sourceId : sourceIds) {
            StringMemorySaver cacheKey = new StringMemorySaver(idType + "|" + sourceId);

            try {
                cacheLock.lock();
                if (internalIdMapCache.containsKey(cacheKey)) {
                    StringMemorySaver cacheValue = internalIdMapCache.get(cacheKey);
                    if (cacheValue == null) {
                        //if null was put in the cache, it means we previously checked the DB and found null
                    } else {
                        String val = cacheValue.toString();
                        ret.put(sourceId, val);
                    }
                } else {
                    idsForDb.add(sourceId);
                }
            } finally {
                cacheLock.unlock();
            }
        }

        if (!idsForDb.isEmpty()) {
            Map<String, String> dbResults = internalIdDal.getDestinationIds(serviceId, idType, idsForDb);

            //add to the cache - note we cache lookup failures too
            try {
                cacheLock.lock();

                for (String sourceId : idsForDb) {
                    StringMemorySaver cacheKey = new StringMemorySaver(idType + "|" + sourceId);

                    String dbVal = dbResults.get(sourceId);
                    if (dbVal == null) {
                        internalIdMapCache.put(cacheKey, null);
                    } else {
                        internalIdMapCache.put(cacheKey, new StringMemorySaver(dbVal));

                        ret.put(sourceId, dbVal);
                    }
                }

            } finally {
                cacheLock.unlock();
            }
        }

        return ret;
    }

    public Resource retrieveResourceForLocalId(ResourceType resourceType, String locallyUniqueId) throws Exception {

        UUID globallyUniqueId = IdHelper.getEdsResourceId(serviceId, resourceType, locallyUniqueId);

        //if we've never mapped the local ID to a EDS UI, then we've never heard of this resource before
        if (globallyUniqueId == null) {
            return null;
        }

        return retrieveResource(resourceType, globallyUniqueId);
    }

    public Resource retrieveResource(ResourceType resourceType, UUID resourceId) throws Exception {

        ResourceWrapper resourceHistory = resourceRepository.getCurrentVersion(serviceId, resourceType.toString(), resourceId);

        //if the resource has been deleted before, we'll have a null entry or one that says it's deleted
        if (resourceHistory == null
                || resourceHistory.isDeleted()) {
            return null;
        }

        String json = resourceHistory.getResourceData();
        return ParserPool.getInstance().parse(json);
    }

    public void cacheProcedureCommentText(CsvCell procedureIdCell, CsvCell procedureCommentTextCell) {

        procedureComments.put(procedureIdCell.getString(), procedureCommentTextCell);
    }

    public CsvCell findProcedureCommentText(CsvCell procedureIdCell) {

        return procedureComments.get(procedureIdCell.getString());
    }

    public void submitToThreadPool(Callable callable) throws Exception {
        if (this.utilityThreadPool == null) {
            int threadPoolSize = ConnectionManager.getPublisherTransformConnectionPoolMaxSize(serviceId);
            this.utilityThreadPool = new ThreadPool(threadPoolSize, 1000, "HomertonHiCsvHelper"); //lower from 50k to save memory
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


//
//    public void cacheNewConsultationChildRelationship(CsvCell encounterIdCell,
//                                                      CsvCell childIdCell,
//                                                      ResourceType childResourceType) throws Exception {
//
//        if (isEmptyOrIsZero(encounterIdCell)) {
//            return;
//        }
//
//        Long encounterId = encounterIdCell.getLong();
//        ReferenceList list = consultationNewChildMap.get(encounterId);
//        if (list == null) {
//            //this is called from multiple threads, so sync and check again before adding
//            synchronized (consultationNewChildMap) {
//                list = consultationNewChildMap.get(encounterId);
//                if (list == null) {
//                    //we know there will only be a single cell, so use this reference list class to save memory
//                    list = new ReferenceListSingleCsvCells();
//                    //list = new ReferenceList();
//                    consultationNewChildMap.put(encounterId, list);
//                }
//            }
//        }
//
//        //ensure a local ID -> Discovery UUID mapping exists, which will end up happening,
//        //but it seems sensible to force it to happen here
//        IdHelper.getOrCreateEdsResourceId(serviceId, childResourceType, childIdCell.getString());
//
//        Reference resourceReference = ReferenceHelper.createReference(childResourceType, childIdCell.getString());
//        list.add(resourceReference, encounterIdCell);
//    }
//
//    public void processRemainingNewConsultationRelationships(FhirResourceFiler fhirResourceFiler) throws Exception {
//
//        //LOG.debug("Remaining consultationNewChildMap items to process: {}", consultationNewChildMap.size());
//
//        for (Long encounterId: consultationNewChildMap.keySet()) {
//            ReferenceList newLinkedItems = consultationNewChildMap.get(encounterId);
//
//            //LOG.debug("newLinkedItems for EncounterId: {} size is: {}", encounterId.toString(), newLinkedItems.size());
//
//            Encounter existingEncounter
//                    = (Encounter)retrieveResourceForLocalId(ResourceType.Encounter, encounterId.toString());
//            if (existingEncounter == null) {
//                //if the encounter has been deleted or does not exist, just skip it
//                continue;
//            }
//
//            //LOG.debug("Existing Encounter found for Id: {} , so repopulating", encounterId.toString());
//
//            EncounterBuilder encounterBuilder = new EncounterBuilder(existingEncounter);
//            ContainedListBuilder containedListBuilder = new ContainedListBuilder(encounterBuilder);
//
//            for (int i=0; i<newLinkedItems.size(); i++) {
//                Reference reference = newLinkedItems.getReference(i);
//                CsvCell[] sourceCells = newLinkedItems.getSourceCells(i);
//
//                Reference globallyUniqueReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(reference, fhirResourceFiler);
//                containedListBuilder.addReference(globallyUniqueReference, sourceCells);
//            }
//
//            //LOG.debug("Saving newLinkedItems for EncounterId: {}", encounterId.toString());
//            fhirResourceFiler.savePatientResource(null, false, encounterBuilder);
//        }
//    }
//
//    public ReferenceList getAndRemoveNewConsultationRelationships(CsvCell encounterIdCell) {
//        Long encounterId = encounterIdCell.getLong();
//        return consultationNewChildMap.remove(encounterId);
//    }
//
//    public void cacheEncounterIdToPersonId(CsvCell encounterIdCell, CsvCell personIdCell) {
//        Long encounterId = encounterIdCell.getLong();
//        String personId = personIdCell.getString();
//        encounterIdToPersonIdMap.put(encounterId, personId);
//    }
//
//    public String findPersonIdFromEncounterId(CsvCell encounterIdCell) throws Exception {
//        Long encounterId = encounterIdCell.getLong();
//        String ret = encounterIdToPersonIdMap.get(encounterId);
//        if (ret == null
//                && !encounterIdToPersonIdMap.containsKey(encounterId)) { //we add null values to the map, so check for the key being present too
//
//            Encounter encounter = (Encounter)retrieveResourceForLocalId(ResourceType.Encounter, encounterIdCell.getString());
//            if (encounter == null) {
//                //if no encounter, then add null to the map to save us hitting the DB repeatedly for the same encounter
//                encounterIdToPersonIdMap.put(encounterId, null);
//
//            } else {
//
//                //we then need to backwards convert the patient UUID to the person ID it came from
//                Reference patientUuidReference = encounter.getPatient();
//                Reference patientPersonIdReference = IdHelper.convertEdsReferenceToLocallyUniqueReference(this, patientUuidReference);
//                ret = ReferenceHelper.getReferenceId(patientPersonIdReference);
//                encounterIdToPersonIdMap.put(encounterId, ret);
//            }
//        }
//
//        return ret;
//    }
//
//    public String getProcedureOrDiagnosisConceptCodeType(CsvCell cell) {
//        if (cell.isEmpty()) {
//            return null;
//        }
//        String conceptCodeIdentifier = cell.getString();
//        int index = conceptCodeIdentifier.indexOf('!');
//        if (index > -1) {
//            String ret = conceptCodeIdentifier.substring(0,index);
//            if (ret.equals(CODE_TYPE_SNOMED)
//                    || ret.equals(CODE_TYPE_ICD_10)) {
//                return ret;
//            } else {
//                throw new IllegalArgumentException("Unexpected code type [" + ret + "]");
//            }
//
//        } else {
//            return null;
//        }
//    }
//
//    public String getProcedureOrDiagnosisConceptCode(CsvCell cell) {
//        if (cell.isEmpty()) {
//            return null;
//        }
//        String conceptCodeIdentifier = cell.getString();
//        int index = conceptCodeIdentifier.indexOf('!');
//        if (index > -1) {
//            return conceptCodeIdentifier.substring(index + 1);
//        } else {
//            return null;
//        }
//    }

    public CernerCodeValueRef lookupCodeRef(CodeValueSet codeSet, CsvCell codeCell) throws Exception {
        String code = codeCell.getString();
        return lookupCodeRef(codeSet, code);
    }

    public CernerCodeValueRef lookupCodeRef(CodeValueSet codeSet, String code) throws Exception {

        String cacheKey = code;
        if (code.equals("0")) {
            //if looking up code zero, this exists in multiple code sets, so add the codeset to the cache key
            cacheKey = codeSet.value() + "|" + cacheKey;
        }

        //Find the code in the cache
        CernerCodeValueRef cernerCodeFromCache = cernerCodes.get(cacheKey);
        if (cernerCodeFromCache != null) {
            return cernerCodeFromCache;
        }

        CernerCodeValueRef cernerCodeFromDB = null;

        //the code is unique across all code sets, EXCEPT for code "0" where this can be repeated
        //between sets. So if the code is "0", perform the lookup using the code set, otherwise just use the code
        if (code.equals("0")) {
            cernerCodeFromDB = cernerCodeValueRefDal.getCodeFromCodeSet(new Long(codeSet.value()), code, serviceId);

        } else {
            cernerCodeFromDB = cernerCodeValueRefDal.getCodeWithoutCodeSet(code, serviceId);
        }

        //TODO - trying to track errors so don't return null from here, but remove once we no longer want to process missing codes
        if (cernerCodeFromDB == null) {
            TransformWarnings.log(LOG, this, "Failed to find Cerner CVREF record for code {} and code set {}", code, codeSet);
            // return new CernerCodeValueRef();
            return null;
        }

        //seem to have whitespace around some of the fields. As a temporary fix, trim them here
        //not required now
        /*if (!Strings.isNullOrEmpty(cernerCodeFromDB.getAliasNhsCdAlias())) {
            cernerCodeFromDB.setAliasNhsCdAlias(cernerCodeFromDB.getAliasNhsCdAlias().trim());
        }
        if (!Strings.isNullOrEmpty(cernerCodeFromDB.getCodeDescTxt())) {
            cernerCodeFromDB.setCodeDescTxt(cernerCodeFromDB.getCodeDescTxt().trim());
        }
        if (!Strings.isNullOrEmpty(cernerCodeFromDB.getCodeDispTxt())) {
            cernerCodeFromDB.setCodeDispTxt(cernerCodeFromDB.getCodeDispTxt().trim());
        }
        if (!Strings.isNullOrEmpty(cernerCodeFromDB.getCodeMeaningTxt())) {
            cernerCodeFromDB.setCodeMeaningTxt(cernerCodeFromDB.getCodeMeaningTxt().trim());
        }*/

        // Add to the cache
        cernerCodes.put(cacheKey, cernerCodeFromDB);

        return cernerCodeFromDB;
    }


//    public static ResourceId getResourceIdFromCache(String resourceIdLookup) {
//        return resourceIds.get(resourceIdLookup);
//    }
//
//    public static void addResourceIdToCache(ResourceId resourceId) {
//        String resourceIdLookup = resourceId.getScopeId()
//                + "|" + resourceId.getResourceType()
//                + "|" + resourceId.getUniqueId() ;
//        resourceIds.put(resourceIdLookup, resourceId);
//    }

//
//    public UUID findPatientIdFromPersonId(CsvCell personIdCell) throws Exception {
//
//        Long personId = personIdCell.getLong();
//
//        //if not in the cache, hit the DB
//        if (!personIdToPatientResourceMap.containsKey(personId)) {
//            //LOG.trace("Person ID not found in cache " + personIdCell.getString());
//
//            String mrn = internalIdDal.getDestinationId(serviceId, InternalIdMap.TYPE_MILLENNIUM_PERSON_ID_TO_MRN, personIdCell.getString());
//            if (mrn == null) {
//                //if we've never received the patient, we won't have a map to its MRN but don't add to the map so if it is created, we'll start working
//                //LOG.trace("Failed to find MRN for person ID " + personIdCell.getString());
//                return null;
//
//            } else {
//
//                //TODO - fix this (if this transform is needed). Change to use normal ID mapping, rather than doing all mapping in the HL7 Receiver database
//                throw new RuntimeException("Code needs fixing");
//                /*ResourceId resourceId = BasisTransformer.getPatientResourceId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, primaryOrgHL7OrgOID, mrn);
//                if (resourceId == null) {
//                    //if we've got the MRN mapping, but haven't actually assigned an ID for it, do so now
//                    resourceId = BasisTransformer.createPatientResourceId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, primaryOrgHL7OrgOID, mrn);
//                    //LOG.trace("Created new resource ID " + resourceId.getResourceId() + " for person ID " + personIdCell.getString());
//                }
//
//                UUID patientId = resourceId.getResourceId();
//                personIdToPatientResourceMap.put(personId, patientId);*/
//                //LOG.trace("Added patient ID " + resourceId.getResourceId() + " to cache " + personIdCell.getString());
//            }
//        }
//
//        return personIdToPatientResourceMap.get(personId);
//    }
//
//    public void cacheParentChildClinicalEventLink(CsvCell childEventIdCell, CsvCell parentEventIdCell) throws Exception {
//        Long parentEventId = parentEventIdCell.getLong();
//        ReferenceList list = clinicalEventChildMap.get(parentEventId);
//        if (list == null) {
//            //we know there will a single CsvCell, so use this reference list class to save memory
//            list = new ReferenceListSingleCsvCells();
//            //list = new ReferenceList();
//            clinicalEventChildMap.put(parentEventId, list);
//        }
//
//        //TODO - fix this so ID mapping is either performed or not performed (whichever is right)
//        if (true) {
//            throw new RuntimeException("Fix code");
//        }
//        /*//we need to map the child ID to a Discovery UUID
//        ResourceId observationResourceId = BasisTransformer.getOrCreateObservationResourceId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, childEventIdCell);
//        Reference reference = ReferenceHelper.createReference(ResourceType.Observation, observationResourceId.getResourceId().toString());
//        list.add(reference, childEventIdCell);*/
//
//    }
//
//    public ReferenceList getAndRemoveClinicalEventParentRelationships(CsvCell parentEventIdCell) {
//        Long parentEventId = parentEventIdCell.getLong();
//        return clinicalEventChildMap.remove(parentEventId);
//    }
//
//
//    /**
//     * as the end of processing all CSV files, there may be some new observations that link
//     * to past parent observations. These linkages are saved against the parent observation,
//     * so we need to retrieve them off the main repository, amend them and save them
//     */
//    public void processRemainingClinicalEventParentChildLinks(FhirResourceFiler fhirResourceFiler) throws Exception {
//        for (Long parentEventId: clinicalEventChildMap.keySet()) {
//            ReferenceList list = clinicalEventChildMap.get(parentEventId);
//            updateExistingObservationWithNewChildLinks(parentEventId, list, fhirResourceFiler);
//        }
//    }
//
//
//    private void updateExistingObservationWithNewChildLinks(Long parentEventId,
//                                                            ReferenceList childResourceRelationships,
//                                                            FhirResourceFiler fhirResourceFiler) throws Exception {
//
//        //convert the parent event ID to a UUID
//        //TODO - apply ID mapping or not, but this needs thinking through
//        if (true) {
//            throw new RuntimeException("Fix code");
//        }
//        /*CsvCell dummyCell = new CsvCell(-1, -1, "" + parentEventId, null);
//        ResourceId observationResourceId = BasisTransformer.getOrCreateObservationResourceId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, dummyCell);
//        Observation observation = (Observation)retrieveResource(ResourceType.Observation, observationResourceId.getResourceId());
//        if (observation == null) {
//            return;
//        }
//
//        ResourceBuilderBase resourceBuilder = new ObservationBuilder(observation);
//
//        boolean changed = false;
//
//        for (int i=0; i<childResourceRelationships.size(); i++) {
//            Reference reference = childResourceRelationships.getReference(i);
//            CsvCell[] sourceCells = childResourceRelationships.getSourceCells(i);
//
//            ObservationBuilder observationBuilder = (ObservationBuilder)resourceBuilder;
//            if (observationBuilder.addChildObservation(reference, sourceCells)) {
//                changed = true;
//            }
//        }
//
//        if (changed) {
//            //make sure to pass in the parameter to bypass ID mapping, since this resource has already been done
//            fhirResourceFiler.savePatientResource(null, false, resourceBuilder);
//        }*/
//    }
//
//    public void saveInternalId(String idType, String sourceId, String destinationId) throws Exception {
//        String cacheKey = idType + "|" + sourceId;
//
//        internalIdDal.save(serviceId, idType, sourceId, destinationId);
//
//        if (internalIdMapCache.containsKey(cacheKey)) {
//            internalIdMapCache.replace(cacheKey, destinationId);
//        } else {
//            internalIdMapCache.put(cacheKey, destinationId);
//        }
//    }
//
//    public String getInternalId(String idType, String sourceId) throws Exception {
//        String cacheKey = idType + "|" + sourceId;
//        if (internalIdMapCache.containsKey(cacheKey)) {
//            return internalIdMapCache.get(cacheKey);
//        }
//
//        String ret = internalIdDal.getDestinationId(serviceId, idType, sourceId);
//
//        if (ret != null) {
//            internalIdMapCache.put(cacheKey, ret);
//        }
//
//        return ret;
//    }
//
//
//    /**
//     * cerner uses zero in place of nulls in a lot of fields, so this method tests for that
//     */
//    public static boolean isEmptyOrIsZero(CsvCell longCell) {
//        if (longCell.isEmpty()) {
//            return true;
//        }
//
//        long val = longCell.getLong();
//        if (val == 0) {
//            return true;
//        }
//
//        return false;
//    }


}
