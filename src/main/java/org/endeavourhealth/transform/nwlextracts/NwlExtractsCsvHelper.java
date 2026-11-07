package org.endeavourhealth.transform.nwlextracts;

import com.google.common.base.Strings;
import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.audit.ExchangeDalI;
import org.endeavourhealth.core.database.dal.audit.models.Exchange;
import org.endeavourhealth.core.database.dal.audit.models.HeaderKeys;
import org.endeavourhealth.core.database.dal.ehr.CoreFilerDalI;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingTargetDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.*;
import org.endeavourhealth.core.database.dal.publisherTransform.CernerCodeValueRefDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.dal.reference.models.SnomedLookup;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.referenceLists.ReferenceList;
import org.endeavourhealth.transform.common.referenceLists.ReferenceListSingleCsvCells;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ObservationBuilder;
import org.endeavourhealth.transform.nwlextracts.cache.*;
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

public class NwlExtractsCsvHelper implements HasServiceSystemAndExchangeIdI {
    private static final Logger LOG = LoggerFactory.getLogger(NwlExtractsCsvHelper.class);

    public static final String CODE_TYPE_SNOMED = "SNOMED";
    public static final String CODE_TYPE_SNOMED_CT = "SNOMED CT";
    public static final String CODE_TYPE_UK_ED_SUBSET = "UK ED Subset";
    public static final String CODE_TYPE_SNOMED_UK_SUBSET = "SNMUKEMED";  // Found in NwlExtracts DIAGN files.
    public static final String CODE_TYPE_ICD_10 = "ICD10WHO";
    public static final String CODE_TYPE_ICD_10_d = "ICD-10";           // Found in NwlExtracts Diagnosis files, v rare.
    public static final String CODE_TYPE_OPCS_4 = "OPCS4";
    public static final String CODE_TYPE_HRG = "HRG";
    public static final String CODE_TYPE_CERNER = "CERNER";
    public static final String CODE_TYPE_PATIENT_CARE = "Patient Care";

    private static SimpleDateFormat DATE_FORMAT_DAILY = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static SimpleDateFormat DATE_FORMAT_BULK = new SimpleDateFormat("yyyyMMdd HH:mm:ss.sss");
    private static SimpleDateFormat DATE_FORMAT_CLEVE = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
    private static SimpleDateFormat DATE_FORMAT_PROCEDURE = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
    private static SimpleDateFormat DATE_FORMAT_PROBLEM = new SimpleDateFormat("dd-MMM-yyyy");


    private static Date cachedEndOfTime = null;
    private static Date cachedStartOfTime = null;

    private CernerCodeValueRefDalI cernerCodeValueRefDal = DalProvider.factoryCernerCodeValueRefDal();
    private InternalIdDalI internalIdDal = DalProvider.factoryInternalIdDal();
    private List<InternalIdMap> internalIdSaveBatch = new ArrayList<>();
    private ResourceDalI resourceRepository = DalProvider.factoryResourceDal();
    private StagingTargetDalI stagingRepository = DalProvider.factoryStagingTargetDalI();
    private CoreFilerDalI coreFilerRepository = DalProvider.factoryCoreFilerDal();

    private Map<String, Integer> odsCodeToOrganizationId = new ConcurrentHashMap<>();

    private Map<String, CernerCodeValueRef> cernerCodes = new ConcurrentHashMap<>();
    private Map<Long, List<CernerCodeValueRef>> cernerCodesBySet = new ConcurrentHashMap<>();
    private Map<StringMemorySaver, StringMemorySaver> internalIdMapCache = new HashMap<>(); //contains nulls so a regular map but uses the cacheLock
    private ReentrantLock cacheLock = new ReentrantLock();
    private Map<Long, SnomedLookup> cleveSnomedConceptMappings = new ConcurrentHashMap<>();
    private String cachedNwlExtractsOrgRefId = null;
    private Date cachedDataDate = null;

    private Map<Integer, Integer> patientCurrentAddressIdMap = new ConcurrentHashMap<>();

    private Map<Long, ReferenceList> clinicalEventChildMap = new ConcurrentHashMap<>();
    private Map<Long, ReferenceList> consultationNewChildMap = new ConcurrentHashMap<>();

    private Date extractDateTime = null;
    private EncounterResourceCache encounterCache = new EncounterResourceCache(this);
   // private EpisodeOfCareResourceCache episodeOfCareCache = new EpisodeOfCareResourceCache(this);
    private LocationResourceCache locationCache = new LocationResourceCache(this);
    private PatientResourceCache patientCache = new PatientResourceCache(this);
    private ProcedurePojoCache procedurecache = new ProcedurePojoCache(this);

    private ThreadPool utilityThreadPool = null;

    private UUID serviceId = null;
    private UUID systemId = null;
    private UUID exchangeId = null;
    private String primaryOrgHL7OrgOID = null;
    private String version = null;

    private Set<String> personIdsToFilterOn = null;

    public NwlExtractsCsvHelper(UUID serviceId, UUID systemId, UUID exchangeId, String primaryOrgHL7OrgOID, String version) {
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
        return NwlExtractsCsvToFhirTransformer.IMPERIALS_RESOURCE_ID_SCOPE;
    }

    public String getHl7ReceiverGlobalScope() {
        return "H";
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

    public CernerCodeValueRef lookupCodeRef(CodeValueSet codeSet, String code) throws Exception {
        return lookupCodeRef(codeSet, CsvCell.factoryDummyWrapper(code));
    }

    /**
     * looks up a CVREF record for its unique code. The CodeSet enum parameter isn't actually used
     * in the lookup in the codes themselves are unique.
     */
    public CernerCodeValueRef lookupCodeRef(CodeValueSet codeSet, CsvCell codeCell) throws Exception {

        String code = codeCell.getString();

        //Need to re-visit this check
         if (code.equals("0")) {
           // SlackHelper.sendSlackMessage(SlackHelper.Channel.Hl7v2Receiver, "ERROR: Trying to look up Cerner CVREF record for code zero " + code + " in code set " + codeSet + "for the ODS Code" + NwlExtractsCsvToFhirTransformer.PRIMARY_ORG_ODS_CODE);
           return null;
        }

        //Find the code in the cache
        CernerCodeValueRef ret = cernerCodes.get(code);
        if (ret != null) {
            return ret;
        }

        ret = cernerCodeValueRefDal.getCodeWithoutCodeSet(code, serviceId);

        //if we fail to look up a CVREF record then something is very wrong, and we can't safely continue since
        //various parts of the transform have logic that depends on the CVREF text/desc.
        if (ret == null) {
          // throw new Exception("Failed to find Cerner CVREF record for code " + code + " in code set " + codeSet);
          //  SlackHelper.sendSlackMessage(SlackHelper.Channel.Hl7v2Receiver, "Failed to find Cerner CVREF record for code " + code + " in code set " + codeSet + "for the ODS Code" + NwlExtractsCsvToFhirTransformer.PRIMARY_ORG_ODS_CODE);
            return null;
        } else {
            // Add to the cache
            cernerCodes.put(code, ret);
            return ret;
        }
    }

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

        Date d = NwlExtractsCsvHelper.parseDate(dateCell);
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

        Date d = NwlExtractsCsvHelper.parseDate(dateCell);
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

    public UUID createResourceIdOrCopyFromHl7Receiver(ResourceType resourceType, String localUniqueId) throws Exception {

        //check our normal ID -> UUID mapping table
        UUID existingResourceId = IdHelper.getEdsResourceId(serviceId, resourceType, localUniqueId);
        if (existingResourceId != null) {
            //LOG.debug("ID already exists for local ID " + localUniqueId);
            return existingResourceId;
        }

        //if no local mapping, check the HL7Receiver DB for the mapping
        ResourceId existingHl7Mapping = null;
        //if the HL7Receiver doesn't have a mapped UUID, then generate one locally and save to the HL7 Receiver DB too
        existingResourceId = IdHelper.getOrCreateEdsResourceId(serviceId, resourceType, localUniqueId);

        return existingResourceId;
    }

    public UUID createResourceIdOrCopyFromHl7Receiver(ResourceType resourceType, ArrayList<String> localUniqueIds) throws Exception {

        //check our normal ID -> UUID mapping table
        UUID existingResourceId = null;
        String mappedId = null;
        for (String localUniqueId:localUniqueIds) {
            existingResourceId= IdHelper.getEdsResourceId(serviceId, resourceType, localUniqueId);
            if(existingResourceId !=null){
                mappedId = localUniqueId;
            }
        }

        if (existingResourceId != null) {
            //LOG.debug("ID already exists for local ID " + localUniqueId);
            for (String localUniqueId:localUniqueIds) {
                if(!localUniqueId.equals(mappedId)){
                    IdHelper.getOrCreateEdsResourceId(serviceId, resourceType, localUniqueId, existingResourceId);
                }
            }
            return existingResourceId;
        }

        //if the HL7Receiver doesn't have a mapped UUID, then generate one locally and save to the HL7 Receiver DB too
        mappedId = localUniqueIds.get(0);
        existingResourceId = IdHelper.getOrCreateEdsResourceId(serviceId, resourceType, mappedId);
        for (String localUniqueId:localUniqueIds) {
            if(!localUniqueId.equals(mappedId)){
                IdHelper.getOrCreateEdsResourceId(serviceId, resourceType, localUniqueId, existingResourceId);
            }
        }

        return existingResourceId;
    }

    //This Core v2 function looks up the Organization.Id value for the provided odsCode
    public Integer findOrganizationIdForOds(String odsCode) throws Exception {

        if (odsCodeToOrganizationId.containsKey(odsCode)) {
            return odsCodeToOrganizationId.get(odsCode);
        }

        Integer orgId = coreFilerRepository.findOrganizationIdFromOdsCode(this.serviceId, odsCode);

        if (orgId != null) {
            odsCodeToOrganizationId.put(odsCode, orgId);
        }

        return orgId;
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

    public Reference createSpecialtyOrganisationReference(CsvCell mainSpecialtyCodeCell) {
        String uniqueId = "Specialty:" + mainSpecialtyCodeCell.getString();
        return ReferenceHelper.createReference(ResourceType.Organization, uniqueId);
    }

    /**
     * we need to know the datetime of the extract as we use that when deleting HL7 Receiver encounters
     */
    public void cacheExtractDateTime(CsvCell extractDateTimeCell) throws Exception {
        if (extractDateTime != null
                || NwlExtractsCsvHelper.isEmptyOrIsEndOfTime(extractDateTimeCell)) {
            return;
        }

        extractDateTime = NwlExtractsCsvHelper.parseDate(extractDateTimeCell);
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

  /*  public EpisodeOfCareResourceCache getEpisodeOfCareCache() {
        return episodeOfCareCache;
    }*/

    public EncounterResourceCache getEncounterCache() {
        return encounterCache;
    }

    public ProcedurePojoCache getProcedureCache() {
        return procedurecache;
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
            this.utilityThreadPool = new ThreadPool(threadPoolSize, 1000, "NwlExtractsCsvHelper"); //lower from 50k to save memory
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



    private static String formatAllcapsMonth(String indate) {
        String first = indate.substring(0, indate.indexOf("-") + 1);
        String month = indate.substring(indate.indexOf("-") + 1, indate.lastIndexOf("-"));
        String rest = indate.substring(indate.lastIndexOf("-"));
        return first + monthToMixedCase(month) + rest;
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
        try {
            return FhirSerializationHelper.deserializeResource(json);
        } catch (Throwable t) {
            throw new Exception("Error deserialising " + resourceType + " " + globallyUniqueId + " (raw ID " + locallyUniqueId + ")", t);
        }
    }

}