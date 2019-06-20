package org.endeavourhealth.transform.barts;

import com.google.common.base.Strings;
import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.audit.ExchangeDalI;
import org.endeavourhealth.core.database.dal.audit.models.Exchange;
import org.endeavourhealth.core.database.dal.audit.models.HeaderKeys;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.hl7receiver.Hl7ResourceIdDalI;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingTargetDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingClinicalEventTarget;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingConditionTarget;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingProcedureTarget;
import org.endeavourhealth.core.database.dal.publisherTransform.CernerCodeValueRefDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerNomenclatureRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.dal.reference.models.SnomedLookup;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.cache.*;
import org.endeavourhealth.transform.barts.schema.CLEVE;
import org.endeavourhealth.transform.barts.transforms.CLEVEPreTransformerOLD;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.referenceLists.ReferenceList;
import org.endeavourhealth.transform.common.referenceLists.ReferenceListSingleCsvCells;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ObservationBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class BartsCsvHelper implements HasServiceSystemAndExchangeIdI, CsvAuditorCallbackI {
    private static final Logger LOG = LoggerFactory.getLogger(BartsCsvHelper.class);

    public static final String CODE_TYPE_SNOMED = "SNOMED";
    public static final String CODE_TYPE_SNOMED_CT = "SNOMED CT";
    public static final String CODE_TYPE_UK_ED_SUBSET = "UK ED Subset";
    public static final String CODE_TYPE_SNOMED_UK_SUBSET ="SNMUKEMED";  // Found in Barts DIAGN files.
    public static final String CODE_TYPE_ICD_10 = "ICD10WHO";
    public static final String CODE_TYPE_ICD_10_d = "ICD-10";           // Found in Barts Diagnosis files, v rare.
    public static final String CODE_TYPE_OPCS_4 = "OPCS4";
    public static final String CODE_TYPE_HRG = "HRG";
    public static final String CODE_TYPE_CERNER = "CERNER";
    public static final String CODE_TYPE_PATIENT_CARE = "Patient Care";
    public static final String SUS_RECORD_TYPE_INPATIENT = "InPatient";
    public static final String SUS_RECORD_TYPE_OUTPATIENT = "OutPatient";
    public static final String SUS_RECORD_TYPE_EMERGENCY = "Emergency";

    private static final String PPREL_TO_RELATIONSHIP_TYPE = "PPREL_ID_TO_TYPE";
    public static final String ENCOUNTER_ID_TO_PERSON_ID = "ENCNTR_ID_TO_PERSON_ID";
    private static final String SURGICAL_CASE_ID_TO_PERSON_ID = "SURCC_ID_TO_PERSON_ID";
    public static final String ENCOUNTER_ID_TO_RESPONSIBLE_PESONNEL_ID = "ENCNTR_ID_TO_RESPONSIBLE_PERSONNEL_ID";

    //the daily files have dates formatted different to the bulks, so we need to support both
    private static SimpleDateFormat DATE_FORMAT_DAILY = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    private static SimpleDateFormat DATE_FORMAT_BULK = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
    private static SimpleDateFormat DATE_FORMAT_CLEVE = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
    private static SimpleDateFormat DATE_FORMAT_PROCEDURE = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
    private static SimpleDateFormat DATE_FORMAT_PROBLEM = new SimpleDateFormat("dd-MMM-yyyy");


    private static Date cachedEndOfTime = null;
    private static Date cachedStartOfTime = null;

    private CernerCodeValueRefDalI cernerCodeValueRefDal = DalProvider.factoryCernerCodeValueRefDal();
    private Hl7ResourceIdDalI hl7ReceiverDal = DalProvider.factoryHL7ResourceDal();
    private InternalIdDalI internalIdDal = DalProvider.factoryInternalIdDal();
    private List<InternalIdMap> internalIdSaveBatch = new ArrayList<>();
    private ResourceDalI resourceRepository = DalProvider.factoryResourceDal();
    private StagingTargetDalI stagingRepository = DalProvider.factoryStagingTargetDalI();

    private Map<String, CernerCodeValueRef> cernerCodes = new ConcurrentHashMap<>();
    private Map<Long, List<CernerCodeValueRef>> cernerCodesBySet = new ConcurrentHashMap<>();
    private Map<Long, CernerNomenclatureRef> nomenclatureCache = new ConcurrentHashMap<>();
    private Map<String, CernerNomenclatureRef> nomenclatureCacheByValueTxt = new ConcurrentHashMap<>();
    private Map<StringMemorySaver, StringMemorySaver> internalIdMapCache = new HashMap<>(); //contains nulls so a regular map but uses the cacheLock
    private ReentrantLock cacheLock = new ReentrantLock();
    private Map<Long, SnomedLookup> cleveSnomedConceptMappings = new ConcurrentHashMap<>();
    private String cachedBartsOrgRefId = null;
    private Date cachedDataDate = null;

    private Map<String, String> snomedDescToConceptCache = new ConcurrentHashMap<>();

    //private Map<Long, String> encounterIdToPersonIdMap = new HashMap<>(); //specifically not a concurrent map because we don't multi-thread and add null values
    //private Map<Long, String> encounterIdToNhsNumberMap = new HashMap<>(); //specifically not a concurrent map because we don't multi-thread and add null values

    private Map<Long, ReferenceList> clinicalEventChildMap = new ConcurrentHashMap<>();
    private Map<Long, ReferenceList> consultationNewChildMap = new ConcurrentHashMap<>();
    //private Map<Long, String> patientRelationshipTypeMap = new HashMap<>();
    private Date extractDateTime = null;
    private EncounterResourceCache encounterCache = new EncounterResourceCache(this);
    private EpisodeOfCareResourceCache episodeOfCareCache = new EpisodeOfCareResourceCache(this);
    private LocationResourceCache locationCache = new LocationResourceCache(this);
    private PatientResourceCache patientCache = new PatientResourceCache(this);
    private ProcedurePojoCache procedurecache = new ProcedurePojoCache(this);
    private SusPatientCache susPatientCache = new SusPatientCache(this);
    private SusPatientTailCache susPatientTailCache = new SusPatientTailCache(this);
    private ThreadPool utilityThreadPool = null;

    private UUID serviceId = null;
    private UUID systemId = null;
    private UUID exchangeId = null;
    private String primaryOrgHL7OrgOID = null;
    private String version = null;

    private Set<String> personIdsToFilterOn = null;

    public BartsCsvHelper(UUID serviceId, UUID systemId, UUID exchangeId, String primaryOrgHL7OrgOID, String version) {
        this.serviceId = serviceId;
        this.systemId = systemId;
        this.exchangeId = exchangeId;
        this.primaryOrgHL7OrgOID = primaryOrgHL7OrgOID;
        this.version = version;
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

    public String getPrimaryOrgHL7OrgOID() {
        return primaryOrgHL7OrgOID;
    }

    /**
     * returns the original date of the data in the exchange (i.e. when actually sent to DDS)
     */
    public Date getDataDate() throws Exception {
        if (cachedDataDate == null) {
            ExchangeDalI exchangeDal = DalProvider.factoryExchangeDal();
            Exchange x = exchangeDal.getExchange(exchangeId);
            cachedDataDate = x.getHeaderAsDate(HeaderKeys.DataDate);

            if (cachedDataDate == null) {
                throw new Exception("Failed to find data date for exchange " + exchangeId);
            }
        }
        return cachedDataDate;
    }


    public String getVersion() {
        return version;
    }

    public String getHl7ReceiverScope() {
        return BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE;
    }

    public String getHl7ReceiverGlobalScope() {
        return "G";
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

    /*public String getInternalId(String idType, String sourceId) throws Exception {
        StringMemorySaver cacheKey = new StringMemorySaver(idType + "|" + sourceId);

        //check the cache - note we cache null lookups in the cache
        try {
            cacheLock.lock();
            if (internalIdMapCache.containsKey(cacheKey)) {
                StringMemorySaver cacheValue = internalIdMapCache.get(cacheKey);
                if (cacheValue == null) {
                    return null;
                } else {
                    return cacheValue.toString();
                }
            }
        } finally {
            cacheLock.unlock();
        }

        String ret = internalIdDal.getDestinationId(serviceId, idType, sourceId);

        //add to the cache - note we cache lookup failures too
        try {
            cacheLock.lock();
            if (ret == null) {
                internalIdMapCache.put(cacheKey, null);
            } else {
                internalIdMapCache.put(cacheKey, new StringMemorySaver(ret));
            }

        } finally {
            cacheLock.unlock();
        }

        return ret;
    }*/

    public Map<String, String> getInternalIds(String idType, Set<String> sourceIds) throws Exception {

        Map<String, String> ret = new HashMap<>();

        //check the cache - note we cache null lookups in the cache
        Set<String> idsForDb = new HashSet<>();
        for (String sourceId: sourceIds) {
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

                for (String sourceId: idsForDb) {
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

    public void processStagingForTargetProcedures() throws Exception {

        stagingRepository.processStagingForTargetProcedures(this.exchangeId, this.serviceId);
    }

    public List<StagingProcedureTarget> retrieveTargetProcedures() throws Exception {

        List<StagingProcedureTarget> ret = stagingRepository.getTargetProcedures(this.exchangeId, this.serviceId);
        return ret;
    }

    public void processStagingForTargetClinicalEvents() throws Exception {

        stagingRepository.processStagingForTargetClinicalEvents(this.exchangeId, this.serviceId);
    }

    public List<StagingClinicalEventTarget> retrieveTargetClinicalEvents() throws Exception {

        List<StagingClinicalEventTarget> ret = stagingRepository.getTargetClinicalEvents(this.exchangeId, this.serviceId);
        return ret;
    }

    public void processStagingForTargetConditions() throws Exception {

        stagingRepository.processStagingForTargetConditions(this.exchangeId, this.serviceId);
    }

    public List<StagingConditionTarget> retrieveTargetConditions() throws Exception {

        List<StagingConditionTarget> ret = stagingRepository.getTargetConditions(this.exchangeId, this.serviceId);
        return ret;
    }

    public List<Resource> retrieveResourceByPatient(UUID patientId) throws Exception {
        List<Resource> ret = new ArrayList<>();
        List<ResourceWrapper> resourceList = resourceRepository.getResourcesByPatient(serviceId, patientId);
        for (ResourceWrapper rw : resourceList) {
            String json = rw.getResourceData();
            ret.add(ParserPool.getInstance().parse(json));
        }
        return ret;
    }

    public Resource retrieveResourceForLocalId(ResourceType resourceType, CsvCell locallyUniqueIdCell) throws Exception {
        return retrieveResourceForLocalId(resourceType, locallyUniqueIdCell.getString());
    }

    public Resource retrieveResourceForLocalId(ResourceType resourceType, String locallyUniqueId) throws Exception {

        UUID globallyUniqueId = IdHelper.getEdsResourceId(serviceId, resourceType, locallyUniqueId);

        //if we've never mapped the local ID to a EDS UI, then we've never heard of this resource before
        if (globallyUniqueId == null) {
            return null;
        }

        return retrieveResourceForUuid(resourceType, globallyUniqueId);
    }

    public Resource retrieveResourceForUuid(ResourceType resourceType, UUID resourceId) throws Exception {

        ResourceWrapper resourceHistory = resourceRepository.getCurrentVersion(serviceId, resourceType.toString(), resourceId);

        //if the resource has been deleted before, we'll have a null entry or one that says it's deleted
        if (resourceHistory == null
                || resourceHistory.isDeleted()) {
            return null;
        }

        String json = resourceHistory.getResourceData();
        try {
            return FhirSerializationHelper.deserializeResource(json);
        } catch (Throwable t) {
            throw new Exception("Error deserialising " + resourceType + " " + resourceId, t);
        }
    }


    /*public Reference createPractitionerReference(String practitionerGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Practitioner, practitionerGuid);
    }*/


    public Reference createPatientReference(CsvCell personIdCell) {
        return ReferenceHelper.createReference(ResourceType.Patient, personIdCell.getString());
    }

    public Reference createPractitionerReference(CsvCell practitionerIdCell) {
        return ReferenceHelper.createReference(ResourceType.Practitioner, practitionerIdCell.getString());
    }

    public Reference createLocationReference(CsvCell locationIdCell) {
        return ReferenceHelper.createReference(ResourceType.Location, locationIdCell.getString());
    }

    public Reference createOrganizationReference(CsvCell organisationIdCell) {
        return ReferenceHelper.createReference(ResourceType.Organization, organisationIdCell.getString());
    }

    public String getProcedureOrDiagnosisConceptCodeType(CsvCell cell) {
        if (cell.isEmpty()) {
            return null;
        }
        String conceptCodeIdentifier = cell.getString();
        int index = conceptCodeIdentifier.indexOf('!');
        if (index > -1) {
            String ret = conceptCodeIdentifier.substring(0, index);
            if (ret.equals(CODE_TYPE_SNOMED)
                    || ret.equals(CODE_TYPE_ICD_10) || ret.equals(CODE_TYPE_ICD_10_d)
                    || ret.equalsIgnoreCase(CODE_TYPE_OPCS_4)
                    || ret.equalsIgnoreCase(CODE_TYPE_HRG)
                    || ret.equals(CODE_TYPE_SNOMED_UK_SUBSET)) {
                return ret;
            } else {
                throw new IllegalArgumentException("Unexpected code type [" + ret + "]");
            }

        } else {
            return null;
        }
    }

    public String getProcedureOrDiagnosisConceptCode(CsvCell cell) {
        if (cell.isEmpty()) {
            return null;
        }
        String conceptCodeIdentifier = cell.getString();
        int index = conceptCodeIdentifier.indexOf('!');
        if (index > -1) {
            return conceptCodeIdentifier.substring(index + 1);
        } else {
            return null;
        }
    }

    /*public void saveLocationUUIDToCache(String locationId, UUID resourceUUID) throws Exception {
        locationIdMap.put(locationId, resourceUUID);
    }

    public UUID lookupLocationUUID(String locationId, FhirResourceFilerI fhirResourceFiler, ParserI parser) throws Exception {
        // Check in cache
        UUID ret = null;
        ret = locationIdMap.get(locationId);
        if (ret != null) {
            return ret;
        }

        ResourceId locationResourceId = getLocationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, locationId);
        if (locationResourceId != null) {
            locationIdMap.put(locationId, locationResourceId.getResourceId());
            return locationResourceId.getResourceId();
        }

        return null;
    }*/

    public String lookupSnomedConceptIdFromDescId(String descId) throws Exception {
        String ret = snomedDescToConceptCache.get(descId);
        if (ret == null) {
            ret = TerminologyService.lookupSnomedConceptForDescriptionId(descId).getConceptCode();
            if (ret == null) {
                TransformWarnings.log(LOG, this, "Failed to find SNOMED concept id for desc ID " + descId);
            }
        }
        return ret;
    }

    public CernerNomenclatureRef lookupNomenclatureRef(Long nomenclatureId) throws Exception {

        CernerNomenclatureRef ret = nomenclatureCache.get(nomenclatureId);
        if (ret == null) {

            ret = cernerCodeValueRefDal.getNomenclatureRefForId(serviceId, nomenclatureId);
            if (ret == null) {
                //don't want to allow failures to continue until I understand why
                throw new TransformException("Failed to find Cerner NOMREF record for ID " + nomenclatureId);
                //TransformWarnings.log(LOG, this, "Failed to find Cerner NOMREF record for ID {}", nomenclatureId);
            } else {
                nomenclatureCache.put(nomenclatureId, ret);
            }
        }

        return ret;
    }

    public CernerNomenclatureRef lookupNomenclatureRefByValueTxt(String valueText) throws Exception {

        CernerNomenclatureRef ret = nomenclatureCacheByValueTxt.get(valueText);
        if (ret == null) {

            ret = cernerCodeValueRefDal.getNomenclatureRefForValueText(serviceId, valueText);
            if (ret == null) {
                //don't want to allow failures to continue until I understand why
                throw new TransformException("Failed to find Cerner NOMREF record for ID " + valueText);
                //TransformWarnings.log(LOG, this, "Failed to find Cerner NOMREF record for ID {}", nomenclatureId);
            } else {
                nomenclatureCacheByValueTxt.put(valueText, ret);
            }
        }

        return ret;
    }

    public CernerCodeValueRef lookupCodeRef(Long codeSet, CsvCell codeCell) throws Exception {
        String code = codeCell.getString();
        return lookupCodeRef(codeSet, code);
    }

    public CernerCodeValueRef lookupCodeRef(Long codeSet, String code) throws Exception {

        String cacheKey = code;
        if (code.equals("0")) {
            //if looking up code zero, this exists in multiple code sets, so add the codeset to the cache key
            cacheKey = codeSet + "|" + cacheKey;
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
            cernerCodeFromDB = cernerCodeValueRefDal.getCodeFromCodeSet(codeSet, code, serviceId);

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

    /*public static ResourceId getResourceIdFromCache(String resourceIdLookup) {
        return resourceIds.get(resourceIdLookup);
    }

    public static void addResourceIdToCache(ResourceId resourceId) {
        String resourceIdLookup = resourceId.getScopeId()
                + "|" + resourceId.getResourceType()
                + "|" + resourceId.getUniqueId() ;
        resourceIds.put(resourceIdLookup, resourceId);
    }*/

    public void saveEncounterIdToPersonId(CsvCell encounterIdCell, CsvCell personIdCell) throws Exception {
        String encounterId = encounterIdCell.getString();
        String personId = personIdCell.getString();
        saveInternalId(ENCOUNTER_ID_TO_PERSON_ID, encounterId, personId);
    }

    public String findPersonIdFromEncounterId(CsvCell encounterIdCell) throws Exception {
        return getInternalId(ENCOUNTER_ID_TO_PERSON_ID, encounterIdCell.getString());
    }

    public void saveEncounterIdToResponsiblePersonnelId(CsvCell encounterIdCell, CsvCell responsiblePersonnelIdCell) throws Exception {
        String encounterId = encounterIdCell.getString();
        String responsiblePersonnelId = responsiblePersonnelIdCell.getString();
        saveInternalId(ENCOUNTER_ID_TO_RESPONSIBLE_PESONNEL_ID, encounterId, responsiblePersonnelId);
    }

    public String findResponsiblePersonnelIdFromEncounterId(CsvCell encounterIdCell) throws Exception {
        return getInternalId(ENCOUNTER_ID_TO_RESPONSIBLE_PESONNEL_ID, encounterIdCell.getString());
    }

    /*public String findPersonIdFromEncounterId(CsvCell encounterIdCell) throws Exception {

        Long encounterId = encounterIdCell.getLong();
        String ret = encounterIdToPersonIdMap.get(encounterId);
        if (ret == null
                && !encounterIdToPersonIdMap.containsKey(encounterId)) { //we add null values to the map, so check for the key being present too

            Encounter encounter = (Encounter) retrieveResourceForLocalId(ResourceType.Encounter, encounterIdCell);
            if (encounter == null) {
                //if no encounter, then add null to the map to save us hitting the DB repeatedly for the same encounter
                encounterIdToPersonIdMap.put(encounterId, null);

            } else {

                //we then need to backwards convert the patient UUID to the person ID it came from
                Reference patientUuidReference = encounter.getPatient();
                Reference patientPersonIdReference = IdHelper.convertEdsReferenceToLocallyUniqueReference(this, patientUuidReference);
                ret = ReferenceHelper.getReferenceId(patientPersonIdReference);
                encounterIdToPersonIdMap.put(encounterId, ret);
            }
        }

        return ret;
    }*/


    /*public String findNhsNumberFromEncounterId(CsvCell encounterIdCell) throws Exception {
        Long encounterId = encounterIdCell.getLong();
        String ret = encounterIdToNhsNumberMap.get(encounterId);
        if (ret == null
                && !encounterIdToNhsNumberMap.containsKey(encounterId)) { //we add null values to the map, so check for the key being present too

            Encounter encounter = (Encounter) retrieveResourceForLocalId(ResourceType.Encounter, encounterIdCell);
            if (encounter == null) {
                //if no encounter, then add null to the map to save us hitting the DB repeatedly for the same encounter
                encounterIdToNhsNumberMap.put(encounterId, null);

            } else {

                //we then need to backwards convert the patient UUID to the person ID it came from
                Reference patientUuidReference = encounter.getPatient();
                Reference patientPersonIdReference = IdHelper.convertEdsReferenceToLocallyUniqueReference(this, patientUuidReference);
                ret = ReferenceHelper.getReferenceId(patientPersonIdReference);
                encounterIdToNhsNumberMap.put(encounterId, ret);
            }
        }

        return ret;
    }*/

    public void cacheParentChildClinicalEventLink(CsvCell childEventIdCell, CsvCell parentEventIdCell) throws Exception {
        Long parentEventId = parentEventIdCell.getLong();
        ReferenceList list = clinicalEventChildMap.get(parentEventId);
        if (list == null) {
            //this is called from multiple threads, so sync and check again before adding
            synchronized (clinicalEventChildMap) {
                list = clinicalEventChildMap.get(parentEventId);
                if (list == null) {
                    //we know there will only be a single cell, so use this reference list class to save memory
                    list = new ReferenceListSingleCsvCells();
                    //list = new ReferenceList();
                    clinicalEventChildMap.put(parentEventId, list);
                }
            }
        }

        //we need to map the child ID to a Discovery UUID
        Reference reference = ReferenceHelper.createReference(ResourceType.Observation, childEventIdCell.getString());
        list.add(reference, childEventIdCell);
    }

    public ReferenceList getAndRemoveClinicalEventParentRelationships(CsvCell parentEventIdCell) {
        Long parentEventId = parentEventIdCell.getLong();
        return clinicalEventChildMap.remove(parentEventId);
    }


    /**
     * as the end of processing all CSV files, there may be some new observations that link
     * to past parent observations. These linkages are saved against the parent observation,
     * so we need to retrieve them off the main repository, amend them and save them
     */
    public void processRemainingClinicalEventParentChildLinks(FhirResourceFiler fhirResourceFiler) throws Exception {
        for (Long parentEventId : clinicalEventChildMap.keySet()) {
            ReferenceList list = clinicalEventChildMap.get(parentEventId);
            updateExistingObservationWithNewChildLinks(parentEventId, list, fhirResourceFiler);
        }

        clinicalEventChildMap.clear();
    }


    private void updateExistingObservationWithNewChildLinks(Long parentEventId,
                                                            ReferenceList childResourceRelationships,
                                                            FhirResourceFiler fhirResourceFiler) throws Exception {

        Observation observation = (Observation) retrieveResourceForLocalId(ResourceType.Observation, parentEventId.toString());
        if (observation == null) {
            return;
        }

        ObservationBuilder observationBuilder = new ObservationBuilder(observation);

        boolean changed = false;

        for (int i = 0; i < childResourceRelationships.size(); i++) {
            Reference reference = childResourceRelationships.getReference(i);
            CsvCell[] sourceCells = childResourceRelationships.getSourceCells(i);

            //the resource ID already ID mapped, so we need to forward map the reference to discovery UUIDs
            Reference globallyUniqueReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(reference, fhirResourceFiler);

            if (observationBuilder.addChildObservation(globallyUniqueReference, sourceCells)) {
                changed = true;
            }
        }

        if (changed) {
            //make sure to pass in the parameter to bypass ID mapping, since this resource has already been done
            fhirResourceFiler.savePatientResource(null, false, observationBuilder);
        }
    }

    /**
     * Cerner uses 31/12/2100 as a "end of time" date, so we check for this to avoid carrying is over into FHIR
     */
    public static boolean isEmptyOrIsEndOfTime(CsvCell dateCell) throws Exception {
        if (dateCell.isEmpty()) {
            return true;
        }

        if (cachedEndOfTime == null) {
            cachedEndOfTime = new SimpleDateFormat("yyyy-MM-dd").parse("2100-12-31");
        }

        Date d = BartsCsvHelper.parseDate(dateCell);
        if (d.equals(cachedEndOfTime)) {
            return true;
        }

        return false;
    }

    public static boolean isEmptyOrIsStartOfTime(CsvCell dateCell) throws Exception {
        if (dateCell.isEmpty()) {
            return true;
        }

        if (cachedStartOfTime == null) {
            cachedStartOfTime = new SimpleDateFormat("yyyy-MM-dd").parse("1800-01-01");
        }

        Date d = BartsCsvHelper.parseDate(dateCell);
        if (d.equals(cachedStartOfTime)) {
            return true;
        }

        return false;
    }

    /**
     * cerner uses zero in place of nulls in a lot of fields, so this method tests for that
     */
    public static boolean isEmptyOrIsZero(CsvCell longCell) {
        if (longCell.isEmpty()) {
            return true;
        }

        long val = longCell.getLong();
        if (val == 0) {
            return true;
        }

        return false;
    }

    /**
     * function to generate UUID mappings from local IDs (e.g. Cerner Person ID) to Discovery UUID, but
     * to carry over any existing mapping from the HL7 Receiver DB, so both ADT and Data Warehouse feeds
     * map the same source concept to the same UUID
     */
    public UUID createResourceIdOrCopyFromHl7Receiver(ResourceType resourceType, String localUniqueId, String hl7ReceiverUniqueId, String hl7ReceiverScope, boolean ignoreFailureToWriteToHl7Db) throws Exception {

        //check our normal ID -> UUID mapping table
        UUID existingResourceId = IdHelper.getEdsResourceId(serviceId, resourceType, localUniqueId);
        if (existingResourceId != null) {
            //LOG.debug("ID already exists for local ID " + localUniqueId);
            return existingResourceId;
        }

        //if no local mapping, check the HL7Receiver DB for the mapping
        ResourceId existingHl7Mapping = hl7ReceiverDal.getResourceId(hl7ReceiverScope, resourceType.toString(), hl7ReceiverUniqueId);
        if (existingHl7Mapping != null) {
            //if the HL7Receiver has a mapped UUID, then store in our local mapping table
            existingResourceId = existingHl7Mapping.getResourceId();
            //LOG.debug("HL7 Receiver already has resource ID " + existingResourceId + " for local ID " + localUniqueId);
            IdHelper.getOrCreateEdsResourceId(serviceId, resourceType, localUniqueId, existingResourceId);
            return existingResourceId;
        }

        //if the HL7Receiver doesn't have a mapped UUID, then generate one locally and save to the HL7 Receiver DB too
        existingResourceId = IdHelper.getOrCreateEdsResourceId(serviceId, resourceType, localUniqueId);

        existingHl7Mapping = new ResourceId();
        existingHl7Mapping.setScopeId(hl7ReceiverScope);
        existingHl7Mapping.setResourceType(resourceType.toString());
        existingHl7Mapping.setUniqueId(hl7ReceiverUniqueId);
        existingHl7Mapping.setResourceId(existingResourceId);

        try {
            hl7ReceiverDal.saveResourceId(existingHl7Mapping);
        } catch (Exception ex) {
            if (!ignoreFailureToWriteToHl7Db) {
                LOG.error("Error saving to HL7 resource_uuid, scope_id = " + existingHl7Mapping.getScopeId() + ", local_id = " + existingHl7Mapping.getUniqueId() + " resource_type = " + existingHl7Mapping.getResourceType() + ", resource_id = " + existingHl7Mapping.getResourceId());
                throw ex;
            }
        }
        //LOG.debug("Generated new UUID " + existingResourceId + " for resource and saved to HL7 receiver DB");

        return existingResourceId;
    }

    public void updateHl7ReceiverWithNewUuid(ResourceType resourceType, String hl7ReceiverUniqueId, String hl7ReceiverScope, UUID resourceUuid) throws Exception {
        ResourceId hl7Mapping = new ResourceId();
        hl7Mapping.setScopeId(hl7ReceiverScope);
        hl7Mapping.setResourceType(resourceType.toString());
        hl7Mapping.setUniqueId(hl7ReceiverUniqueId);
        hl7Mapping.setResourceId(resourceUuid);

        try {
            hl7ReceiverDal.updateResourceId(hl7Mapping);
        } catch (Exception ex) {
            LOG.error("Error updating HL7 resource_uuid, scope_id = " + hl7Mapping.getScopeId() + ", local_id = " + hl7Mapping.getUniqueId() + " resource_type = " + hl7Mapping.getResourceType() + ", resource_id = " + hl7Mapping.getResourceId());
            throw ex;
        }
    }


    /**
     * looks up the UUID for the Organization resource that represents Barts itself
     */
    public UUID findOrganizationResourceIdForBarts() throws Exception {
        String orgRefId = findOrgRefIdForBarts();
        return IdHelper.getEdsResourceId(serviceId, ResourceType.Organization, orgRefId);
    }

    public String findOrgRefIdForBarts() throws Exception {

        //if already cached
        if (cachedBartsOrgRefId != null) {
            return cachedBartsOrgRefId;
        }

        //if not cached, we need to look it up its ORGREF ID using its ODS code
        cachedBartsOrgRefId = getInternalId(InternalIdMap.TYPE_CERNER_ODS_CODE_TO_ORG_ID, BartsCsvToFhirTransformer.PRIMARY_ORG_ODS_CODE);
        if (cachedBartsOrgRefId == null) {
            throw new TransformException("Failed to find ORGREF ID for ODS code " + BartsCsvToFhirTransformer.PRIMARY_ORG_ODS_CODE);
        }

        return cachedBartsOrgRefId;
    }

    /**
     * bulks and deltas have different date formats, so use this to handle both
     */
    public static Date parseDate(CsvCell cell) throws ParseException {

        if (cell.isEmpty()) {
            return null;
        }

        Date d;
        boolean adjustForBst;

        String dateString = cell.getString();

        //quick and dirty check first before we try parsing. Only this format has dots in it.
        if (dateString.contains(".")) {
            d = DATE_FORMAT_BULK.parse(dateString);
            adjustForBst = false;

        } else {
            try {
                d = DATE_FORMAT_DAILY.parse(dateString);
                adjustForBst = true;

            } catch (ParseException ex) {
                try {
                    //bulk extracts used a different date format and weren't affected by BST issue
                    adjustForBst = false;
                    d = DATE_FORMAT_BULK.parse(dateString);

                } catch (ParseException ex2) {
                    try {
                        //fixed width files (e.g. procedure) use a third format and aren't affected by BST issue
                        String date3 = formatAllcapsMonth(dateString);
                        d = DATE_FORMAT_PROCEDURE.parse(date3);
                        adjustForBst = false;
                    } catch (ParseException ex3) {
                        try {
                            d = DATE_FORMAT_PROBLEM.parse(dateString);
                            adjustForBst = false;

                        } catch (ParseException ex4) {
                            //I have no idea if the weird CLEVE dates are affected by the BST issue or not
                            //so we need to investigate to find out if they are or not. But I don't have time to
                            //work that out now, so pushing this back until we actually start processing the CLEVE files
                            d = DATE_FORMAT_CLEVE.parse(dateString);
                            adjustForBst = false;
                        }
                    }
                }
            }
        }

        //DAB-75 data entered during BST is an hour out in the extracts, so we need to move forwards to correct it
        if (adjustForBst) {
            if (BstHelper.isBst(d)) {
                Calendar cal = BstHelper.borrowCalendar(); //cheaper than creating a new calendar
                cal.setTime(d);
                cal.add(Calendar.HOUR_OF_DAY, 1);
                d = cal.getTime();
                BstHelper.returnCalendar(cal);
            }
        }

        return d;
    }
    /*public static Date parseDate(CsvCell cell) throws ParseException {

        if (cell.isEmpty()) {
            return null;
        }

        String dateString = cell.getString();
        // try to avoid expected ParseExceptions by guessing the correct dateFormat
        if (dateString.contains(".")) {
            try {
                return DATE_FORMAT_BULK.parse(dateString);
            } catch (ParseException ex) {
                try {
                    return DATE_FORMAT_DAILY.parse(dateString);
                } catch (ParseException ex2) {
                    return DATE_FORMAT_CLEVE.parse(dateString);
                }
            }

        } else {
            try {
                return DATE_FORMAT_DAILY.parse(dateString);
            } catch (ParseException ex) {
                try {
                    return DATE_FORMAT_BULK.parse(dateString);
                } catch (ParseException ex2) {
                    try {
                        return DATE_FORMAT_CLEVE.parse(dateString);
                    } catch (ParseException ex3) {
                        String date3 = formatAllcapsMonth(dateString);
                        return DATE_FORMAT_CLEVE2.parse(date3);

                    }
                }

            }
        }
    }*/


    private static String monthToMixedCase(String month) {
        switch (month) {
            case "JAN":
                return "Jan";
            case "FEB":
                return "Feb";
            case "MAR":
                return "Mar";
            case "APR":
                return "Apr";
            case "MAY":
                return "May";
            case "JUN":
                return "Jun";
            case "JUL":
                return "Jul";
            case "AUG":
                return "Aug";
            case "SEP":
                return "Sep";
            case "OCT":
                return "Oct";
            case "NOV":
                return "Nov";
            case "DEC":
                return "Dec";
        }
        return "unknown";
    }

    /**
     * we store the relationship type for each PPREL in the internal ID map table
     */
    public String getPatientRelationshipType(CsvCell personIdCell, CsvCell relationshipIdCell) throws Exception {

        String sourceId = personIdCell.getString() + ":" + relationshipIdCell.getString();
        String destId = getInternalId(PPREL_TO_RELATIONSHIP_TYPE, sourceId);
        if (Strings.isNullOrEmpty(destId)) {
            return null;
        }

        CernerCodeValueRef codeRef = lookupCodeRef(CodeValueSet.RELATIONSHIP_TO_PATIENT, destId);
        if (codeRef == null) {
            return null;
        }

        return codeRef.getCodeDescTxt();
    }

    public void savePatientRelationshipType(CsvCell personIdCell, CsvCell relationshipIdCell, CsvCell typeCode) throws Exception {
        if (BartsCsvHelper.isEmptyOrIsZero(typeCode)) {
            return;
        }

        String sourceId = personIdCell.getString() + ":" + relationshipIdCell.getString();
        String destId = typeCode.getString();

        //only save if it's different to the current
        String existingDestId = getInternalId(PPREL_TO_RELATIONSHIP_TYPE, sourceId);
        if (existingDestId == null
                || !existingDestId.equals(destId)) {

            saveInternalId(PPREL_TO_RELATIONSHIP_TYPE, sourceId, destId);
        }
    }

    /*
    public void cachePatientRelationshipType(CsvCell relationshipIdCell, String typeDesc) {
        Long relationshipId = relationshipIdCell.getLong();
        patientRelationshipTypeMap.put(relationshipId, typeDesc);
    }

    public String getPatientRelationshipType(CsvCell relationshipIdCell, CsvCell personIdCell) throws Exception {
        //check the cache first, which will contain the new relationships from this Exchange
        Long relationshipId = relationshipIdCell.getLong();
        String ret = patientRelationshipTypeMap.get(relationshipId);
        if (ret != null) {
            LOG.debug("Found relationship type in cache");
            return ret;
        }

        //if not in the cache, check the DB, which means retrieving the patient
        Patient patient = (Patient)retrieveResourceForLocalId(ResourceType.Patient, personIdCell);
        if (patient == null) {
            LOG.debug("Failed to find patient for ID " + personIdCell.getString());
            return null;
        }

        PatientBuilder patientBuilder = new PatientBuilder(patient);
        Patient.ContactComponent relationship = PatientContactBuilder.findExistingContactPoint(patientBuilder, relationshipIdCell.getString());
        if (relationship == null) {
            LOG.debug("Failed to find relationship with person " + relationshipIdCell.getString());
            return null;
        }

        if (!relationship.hasRelationship()) {
            LOG.debug("Relationship doesn't have a type");
            return null;
        }

        CodeableConcept codeableConcept = relationship.getRelationship().get(0);
        return codeableConcept.getText();
    }*/

    public void setEpisodeReferenceOnEncounter(EpisodeOfCareBuilder episodeOfCareBuilder, EncounterBuilder encounterBuilder, FhirResourceFiler fhirResourceFiler) throws Exception {

        boolean episodeIdMapped = episodeOfCareBuilder.isIdMapped();
        Reference episodeReference = ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeOfCareBuilder.getResourceId());

        boolean encounterIdMapped = encounterBuilder.isIdMapped();

        if (encounterIdMapped == episodeIdMapped) {
            //if both are ID mapped (or both aren't) then no translation is needed

        } else if (encounterIdMapped) {
            //if encounter is ID mapped and the episode isn't, then we need to translate
            episodeReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(episodeReference, fhirResourceFiler);

        } else {
            //if encounter isn't ID mapped, but episode is, then we need to translate
            episodeReference = IdHelper.convertEdsReferenceToLocallyUniqueReference(fhirResourceFiler, episodeReference);
        }

        encounterBuilder.setEpisodeOfCare(episodeReference);
    }

    public Reference createSpecialtyOrganisationReference(CsvCell mainSpecialtyCodeCell) {
        String uniqueId = "Specialty:" + mainSpecialtyCodeCell.getString();
        return ReferenceHelper.createReference(ResourceType.Organization, uniqueId);
    }

    /**
     * we need to know the datetime of the extract as we use that when deleting HL7 Receiver encounters
     */
    public void cacheExtractDateTime(CsvCell extractDateTimeCell) throws Exception {
        if (extractDateTime != null
                || BartsCsvHelper.isEmptyOrIsEndOfTime(extractDateTimeCell)) {
            return;
        }

        extractDateTime = BartsCsvHelper.parseDate(extractDateTimeCell);
    }

    public Date getExtractDateTime() {
        return extractDateTime;
    }

    public PatientResourceCache getPatientCache() {
        return patientCache;
    }

    public LocationResourceCache getLocationCache() {
        return locationCache;
    }

    public EpisodeOfCareResourceCache getEpisodeOfCareCache() {
        return episodeOfCareCache;
    }

    public EncounterResourceCache getEncounterCache() {
        return encounterCache;
    }

    public ProcedurePojoCache getProcedureCache() {
        return procedurecache;
    }

    public SusPatientCache getSusPatientCache() {
        return susPatientCache;
    }

    public SusPatientTailCache getSusPatientTailCache() {
        return susPatientTailCache;
    }

    public void cacheNewConsultationChildRelationship(CsvCell encounterIdCell,
                                                      CsvCell childIdCell,
                                                      ResourceType childResourceType) throws Exception {

        if (isEmptyOrIsZero(encounterIdCell)) {
            return;
        }

        Long encounterId = encounterIdCell.getLong();
        ReferenceList list = consultationNewChildMap.get(encounterId);
        if (list == null) {
            //this is called from multiple threads, so sync and check again before adding
            synchronized (consultationNewChildMap) {
                list = consultationNewChildMap.get(encounterId);
                if (list == null) {
                    //we know there will only be a single cell, so use this reference list class to save memory
                    list = new ReferenceListSingleCsvCells();
                    //list = new ReferenceList();
                    consultationNewChildMap.put(encounterId, list);
                }
            }
        }

        //ensure a local ID -> Discovery UUID mapping exists, which will end up happening,
        //but it seems sensible to force it to happen here
        IdHelper.getOrCreateEdsResourceId(serviceId, childResourceType, childIdCell.getString());

        Reference resourceReference = ReferenceHelper.createReference(childResourceType, childIdCell.getString());
        list.add(resourceReference, encounterIdCell);
    }

    public ReferenceList getAndRemoveNewConsultationRelationships(CsvCell encounterIdCell) {
        Long encounterId = encounterIdCell.getLong();
        return consultationNewChildMap.remove(encounterId);
    }

    public void processRemainingNewConsultationRelationships(FhirResourceFiler fhirResourceFiler) throws Exception {
        for (Long encounterId : consultationNewChildMap.keySet()) {
            ReferenceList newLinkedItems = consultationNewChildMap.get(encounterId);

            Encounter existingEncounter = (Encounter) retrieveResourceForLocalId(ResourceType.Encounter, encounterId.toString());
            if (existingEncounter == null) {
                //if the problem has been deleted, just skip it
                continue;
            }

            EncounterBuilder encounterBuilder = new EncounterBuilder(existingEncounter);
            ContainedListBuilder containedListBuilder = new ContainedListBuilder(encounterBuilder);

            for (int i = 0; i < newLinkedItems.size(); i++) {
                Reference reference = newLinkedItems.getReference(i);
                CsvCell[] sourceCells = newLinkedItems.getSourceCells(i);

                Reference globallyUniqueReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(reference, fhirResourceFiler);
                containedListBuilder.addReference(globallyUniqueReference, sourceCells);
            }

            fhirResourceFiler.savePatientResource(null, false, encounterBuilder);
        }

        consultationNewChildMap.clear();
    }

    public List<CernerCodeValueRef> getCernerCodesForSet(Long set) throws Exception {
        List<CernerCodeValueRef> ret = cernerCodesBySet.get(set);
        if (ret == null) {
            ret = cernerCodeValueRefDal.getCodesForCodeSet(serviceId, set);
            cernerCodesBySet.put(set, ret);
        }
        return ret;
    }

    public void cacheCleveSnomedConceptId(CsvCell eventIdCell, SnomedLookup snomedLookup) {
        Long id = eventIdCell.getLong();
        cleveSnomedConceptMappings.put(id, snomedLookup);
    }

    public SnomedLookup getAndRemoveCleveSnomedConceptId(CsvCell eventIdCell) {
        Long id = eventIdCell.getLong();
        return cleveSnomedConceptMappings.remove(id);
    }

    public boolean processRecordFilteringOnPatientId(AbstractCsvParser parser) throws TransformException {
        CsvCell personIdCell = parser.getCell("PERSON_ID");
        if (personIdCell == null) {
            personIdCell = parser.getCell("#PERSON_ID");

            //if nothing that looks like a person ID, process the record
            if (personIdCell == null) {
                throw new TransformException("No PERSON_ID column on parser " + parser.getFilePath());
            }
        }

        String personId = personIdCell.getString();
        return processRecordFilteringOnPatientId(personId);
    }

    public boolean processRecordFilteringOnPatientId(String personId) {

        if (personIdsToFilterOn == null) {
            String filePath = TransformConfig.instance().getCernerPatientIdFile();
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


    public void submitToThreadPool(Callable callable) throws Exception {
        if (this.utilityThreadPool == null) {
            int threadPoolSize = ConnectionManager.getPublisherTransformConnectionPoolMaxSize(serviceId);
            this.utilityThreadPool = new ThreadPool(threadPoolSize, 50000);
        }

        List<ThreadPoolError> errors = utilityThreadPool.submit(callable);
        AbstractCsvCallable.handleErrors(errors);
    }

    public void waitUntilThreadPoolIsEmpty() throws Exception {

        //commit any unsaved internal IDs to the DB
        saveInternalIdBatch(internalIdSaveBatch);

        if (this.utilityThreadPool != null) {
            List<ThreadPoolError> errors = utilityThreadPool.waitUntilEmpty();
            AbstractCsvCallable.handleErrors(errors);
        }
    }

    public void stopThreadPool() throws Exception {

        //commit any unsaved internal IDs to the DB
        saveInternalIdBatch(internalIdSaveBatch);

        if (this.utilityThreadPool != null) {
            List<ThreadPoolError> errors = utilityThreadPool.waitAndStop();
            AbstractCsvCallable.handleErrors(errors);
        }
    }

    /**
     * used to selectively filter CSV records so that we don't audit every CLEVE record received, since we
     * only process a relatively small number of them
     */
    @Override
    public boolean shouldAuditRecord(ParserI parser) throws Exception {
        if (parser instanceof CLEVE) {
            CLEVE cleveParser = (CLEVE) parser;
            return CLEVEPreTransformerOLD.shouldTransformOrAuditRecord(cleveParser, this);
        }

        //audit every record of any other files
        return true;
    }

    private static String formatAllcapsMonth(String indate) {
        String first = indate.substring(0, indate.indexOf("-") + 1);
        String month = indate.substring(indate.indexOf("-") + 1, indate.lastIndexOf("-"));
        String rest = indate.substring(indate.lastIndexOf("-"));
        return first + monthToMixedCase(month) + rest;
    }


    public void saveSurgicalCaseIdToPersonId(CsvCell surgicalCaseIdCell, CsvCell personIdCell) throws Exception {
        String surgicalCaseId = surgicalCaseIdCell.getString();
        String personId = personIdCell.getString();
        saveInternalId(SURGICAL_CASE_ID_TO_PERSON_ID, surgicalCaseId, personId);
    }

    public String findPersonIdFromSurgicalCaseId(CsvCell surgicalCaseIdCell) throws Exception {
        return getInternalId(SURGICAL_CASE_ID_TO_PERSON_ID, surgicalCaseIdCell.getString());
    }

}
