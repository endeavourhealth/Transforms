package org.endeavourhealth.transform.homerton;

import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.admin.ServiceDalI;
import org.endeavourhealth.core.database.dal.admin.models.Service;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.CernerCodeValueRefDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.HasServiceSystemAndExchangeIdI;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.referenceLists.ReferenceList;
import org.endeavourhealth.transform.common.referenceLists.ReferenceListSingleCsvCells;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.homerton.cache.EncounterResourceCache;
import org.endeavourhealth.transform.homerton.cache.LocationResourceCache;
import org.endeavourhealth.transform.homerton.cache.PatientResourceCache;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class HomertonCsvHelper implements HasServiceSystemAndExchangeIdI {
    private static final Logger LOG = LoggerFactory.getLogger(HomertonCsvHelper.class);

    public static final String CODE_TYPE_SNOMED = "SNOMED CT";
    public static final String CODE_TYPE_ICD_10 = "ICD-10";
    public static final String CODE_TYPE_OPCS_4 = "OPCS4";

    private static CernerCodeValueRefDalI cernerCodeValueRefDalI = DalProvider.factoryCernerCodeValueRefDal();
    private static HashMap<String, CernerCodeValueRef> cernerCodes = new HashMap<>();
    private static HashMap<String, ResourceId> resourceIds = new HashMap<>();
    private static HashMap<String, String> internalIdMapCache = new HashMap<>();

    //non-static caches
    private Map<Long, UUID> encounterIdToEncounterResourceMap = new HashMap<>();
    private Map<Long, UUID> encounterIdToPatientResourceMap = new HashMap<>();
    private Map<Long, UUID> personIdToPatientResourceMap = new HashMap<>();
    private Map<Long, ReferenceList> clinicalEventChildMap = new HashMap<>();
    private Map<Long, ReferenceList> consultationNewChildMap = new ConcurrentHashMap<>();
    private Map<Long, String> encounterIdToPersonIdMap = new HashMap<>(); //specifically not a concurrent map because we don't multi-thread and add null values
    private Map<String, CsvCell> codeValueNHSAlias = new HashMap<>();

    private PatientResourceCache patientCache = new PatientResourceCache();
    private EncounterResourceCache encounterCache = new EncounterResourceCache();
    private LocationResourceCache locationCache = new LocationResourceCache();

    private InternalIdDalI internalIdDal = DalProvider.factoryInternalIdDal();
    private ResourceDalI resourceRepository = DalProvider.factoryResourceDal();
    private ServiceDalI serviceRepository = DalProvider.factoryServiceDal();
    private UUID serviceId = null;
    private UUID systemId = null;
    private UUID exchangeId = null;
    private String primaryOrgHL7OrgOID = null;
    private String version = null;

    public HomertonCsvHelper(UUID serviceId, UUID systemId, UUID exchangeId, String primaryOrgHL7OrgOID, String version) {
        this.serviceId = serviceId;
        this.systemId = systemId;
        this.exchangeId = exchangeId;
        this.primaryOrgHL7OrgOID = primaryOrgHL7OrgOID;
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

    public String getPrimaryOrgHL7OrgOID() {
        return primaryOrgHL7OrgOID;
    }


    public String getVersion() {
        return version;
    }

    public PatientResourceCache getPatientCache() { return patientCache; }

    public EncounterResourceCache getEncounterCache() { return encounterCache; }

    public LocationResourceCache getLocationCache() { return locationCache; }

    public Service getService (UUID id) throws Exception { return serviceRepository.getById(id);}

    // if the resource is already filed and has been retrieved from the DB, the sourceId will differ from the
    // saved (mapped) resource Id
    public boolean isResourceIdMapped (String sourceId, DomainResource resource) {
        return !resource.getId().equals(sourceId);
    }

    public Reference createOrganisationReference(String organizationGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Organization, organizationGuid);
    }

    public Reference createLocationReference(String locationGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Location, locationGuid);
    }

    public Reference createPractitionerReference(String practitionerGuid) {
        return ReferenceHelper.createReference(ResourceType.Practitioner, practitionerGuid);
    }

    public List<Resource> retrieveResourceByPatient(UUID patientId) throws Exception {
        List<Resource> ret = null;
        List<ResourceWrapper> resourceList = resourceRepository.getResourcesByPatient(serviceId, patientId);
        for (ResourceWrapper rw : resourceList) {
            if (ret == null) {
                ret = new ArrayList<>();
            }
            String json = rw.getResourceData();
            ret.add(ParserPool.getInstance().parse(json));
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

    public void processRemainingNewConsultationRelationships(FhirResourceFiler fhirResourceFiler) throws Exception {

        //LOG.debug("Remaining consultationNewChildMap items to process: {}", consultationNewChildMap.size());

        for (Long encounterId: consultationNewChildMap.keySet()) {
            ReferenceList newLinkedItems = consultationNewChildMap.get(encounterId);

            //LOG.debug("newLinkedItems for EncounterId: {} size is: {}", encounterId.toString(), newLinkedItems.size());

            Encounter existingEncounter
                    = (Encounter)retrieveResourceForLocalId(ResourceType.Encounter, encounterId.toString());
            if (existingEncounter == null) {
                //if the encounter has been deleted or does not exist, just skip it
                continue;
            }

            //LOG.debug("Existing Encounter found for Id: {} , so repopulating", encounterId.toString());

            EncounterBuilder encounterBuilder = new EncounterBuilder(existingEncounter);
            ContainedListBuilder containedListBuilder = new ContainedListBuilder(encounterBuilder);

            for (int i=0; i<newLinkedItems.size(); i++) {
                Reference reference = newLinkedItems.getReference(i);
                CsvCell[] sourceCells = newLinkedItems.getSourceCells(i);

                Reference globallyUniqueReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(reference, fhirResourceFiler);
                containedListBuilder.addContainedListItem(globallyUniqueReference, sourceCells);
            }

            //LOG.debug("Saving newLinkedItems for EncounterId: {}", encounterId.toString());
            fhirResourceFiler.savePatientResource(null, false, encounterBuilder);
        }
    }

    public ReferenceList getAndRemoveNewConsultationRelationships(CsvCell encounterIdCell) {
        Long encounterId = encounterIdCell.getLong();
        return consultationNewChildMap.remove(encounterId);
    }

    public void cacheEncounterIdToPersonId(CsvCell encounterIdCell, CsvCell personIdCell) {
        Long encounterId = encounterIdCell.getLong();
        String personId = personIdCell.getString();
        encounterIdToPersonIdMap.put(encounterId, personId);
    }

    public String findPersonIdFromEncounterId(CsvCell encounterIdCell) throws Exception {
        Long encounterId = encounterIdCell.getLong();
        String ret = encounterIdToPersonIdMap.get(encounterId);
        if (ret == null
                && !encounterIdToPersonIdMap.containsKey(encounterId)) { //we add null values to the map, so check for the key being present too

            Encounter encounter = (Encounter)retrieveResourceForLocalId(ResourceType.Encounter, encounterIdCell.getString());
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
    }

    public String getProcedureOrDiagnosisConceptCodeType(CsvCell cell) {
        if (cell.isEmpty()) {
            return null;
        }
        String conceptCodeIdentifier = cell.getString();
        int index = conceptCodeIdentifier.indexOf('!');
        if (index > -1) {
            String ret = conceptCodeIdentifier.substring(0,index);
            if (ret.equals(CODE_TYPE_SNOMED)
                    || ret.equals(CODE_TYPE_ICD_10)) {
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

    public CernerCodeValueRef lookupCodeRef(String code) throws Exception {
        return lookupCodeRef(0L, code);
    }

    public void cacheCodeNHSAlias(String codeValue, CsvCell codeNHSAliasCell) {
        codeValueNHSAlias.put(codeValue, codeNHSAliasCell);
    }

    public CsvCell findCodeNHSAlias(String codeValue) {
        return codeValueNHSAlias.get(codeValue);
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
            cernerCodeFromDB = cernerCodeValueRefDalI.getCodeFromCodeSet(codeSet, code, serviceId);

        } else {
            cernerCodeFromDB = cernerCodeValueRefDalI.getCodeWithoutCodeSet(code, serviceId);
        }

        if (cernerCodeFromDB == null)
            return null;

        // Add to the cache
        cernerCodes.put(cacheKey, cernerCodeFromDB);

        return cernerCodeFromDB;
    }

    public static ResourceId getResourceIdFromCache(String resourceIdLookup) {
        return resourceIds.get(resourceIdLookup);
    }

    public static void addResourceIdToCache(ResourceId resourceId) {
        String resourceIdLookup = resourceId.getScopeId()
                + "|" + resourceId.getResourceType()
                + "|" + resourceId.getUniqueId() ;
        resourceIds.put(resourceIdLookup, resourceId);
    }


//    public UUID findEncounterResourceIdFromEncounterId(CsvCell encounterIdCell) throws Exception {
//        ensureEncounterIdsAreCached(encounterIdCell);
//        Long encounterId = encounterIdCell.getLong();
//        return encounterIdToEnconterResourceMap.get(encounterId);
//    }
//
//    public UUID findPatientIdFromEncounterId(CsvCell encounterIdCell) throws Exception {
//        ensureEncounterIdsAreCached(encounterIdCell);
//        Long encounterId = encounterIdCell.getLong();
//        return encounterIdToPatientResourceMap.get(encounterId);
//    }
//
//    public void cacheEncounterIds(CsvCell encounterIdCell, Encounter encounter) {
//        Long encounterId = encounterIdCell.getLong();
//
//        String id = encounter.getId();
//        UUID encounterUuid = UUID.fromString(id);
//        encounterIdToEnconterResourceMap.put(encounterId, encounterUuid);
//
//        Reference patientReference = encounter.getPatient();
//        ReferenceComponents comps = ReferenceHelper.getReferenceComponents(patientReference);
//        UUID patientUuid = UUID.fromString(comps.getId());
//        encounterIdToPatientResourceMap.put(encounterId, patientUuid);
//    }
//
//    private void ensureEncounterIdsAreCached(CsvCell encounterIdCell) throws Exception {
//
//        Long encounterId = encounterIdCell.getLong();
//
//        //if already cached, return
//        if (encounterIdToEnconterResourceMap.containsKey(encounterId)) {
//            return;
//        }
//
//        ResourceId encounterResourceId = HomertonBasisTransformer.getEncounterResourceId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, encounterIdCell.getString());
//        if (encounterResourceId == null) {
//            //add nulls to the map so we don't keep hitting the DB
//            encounterIdToEnconterResourceMap.put(encounterId, null);
//            encounterIdToPatientResourceMap.put(encounterId, null);
//            return;
//        }
//
//        Encounter fhirEncounter = (Encounter)retrieveResource(ResourceType.Encounter, encounterResourceId.getResourceId());
//        if (fhirEncounter == null) {
//            //if encounter has been deleted, add nulls to the map so we don't keep hitting the DB
//            encounterIdToEnconterResourceMap.put(encounterId, null);
//            encounterIdToPatientResourceMap.put(encounterId, null);
//            return;
//        }
//
//        cacheEncounterIds(encounterIdCell, fhirEncounter);
//    }


    public UUID findPatientIdFromPersonId(CsvCell personIdCell) throws Exception {

        Long personId = personIdCell.getLong();

        //if not in the cache, hit the DB
        if (!personIdToPatientResourceMap.containsKey(personId)) {
            //LOG.trace("Person ID not found in cache " + personIdCell.getString());

            String mrn = internalIdDal.getDestinationId(serviceId, InternalIdMap.TYPE_MILLENNIUM_PERSON_ID_TO_MRN, personIdCell.getString());
            if (mrn == null) {
                //if we've never received the patient, we won't have a map to its MRN but don't add to the map so if it is created, we'll start working
                //LOG.trace("Failed to find MRN for person ID " + personIdCell.getString());
                return null;

            } else {

                //TODO - fix this (if this transform is needed). Change to use normal ID mapping, rather than doing all mapping in the HL7 Receiver database
                throw new RuntimeException("Code needs fixing");
                /*ResourceId resourceId = BasisTransformer.getPatientResourceId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, primaryOrgHL7OrgOID, mrn);
                if (resourceId == null) {
                    //if we've got the MRN mapping, but haven't actually assigned an ID for it, do so now
                    resourceId = BasisTransformer.createPatientResourceId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, primaryOrgHL7OrgOID, mrn);
                    //LOG.trace("Created new resource ID " + resourceId.getResourceId() + " for person ID " + personIdCell.getString());
                }

                UUID patientId = resourceId.getResourceId();
                personIdToPatientResourceMap.put(personId, patientId);*/
                //LOG.trace("Added patient ID " + resourceId.getResourceId() + " to cache " + personIdCell.getString());
            }
        }

        return personIdToPatientResourceMap.get(personId);
    }

    public void cacheParentChildClinicalEventLink(CsvCell childEventIdCell, CsvCell parentEventIdCell) throws Exception {
        Long parentEventId = parentEventIdCell.getLong();
        ReferenceList list = clinicalEventChildMap.get(parentEventId);
        if (list == null) {
            //we know there will a single CsvCell, so use this reference list class to save memory
            list = new ReferenceListSingleCsvCells();
            //list = new ReferenceList();
            clinicalEventChildMap.put(parentEventId, list);
        }

        //TODO - fix this so ID mapping is either performed or not performed (whichever is right)
        if (true) {
            throw new RuntimeException("Fix code");
        }
        /*//we need to map the child ID to a Discovery UUID
        ResourceId observationResourceId = BasisTransformer.getOrCreateObservationResourceId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, childEventIdCell);
        Reference reference = ReferenceHelper.createReference(ResourceType.Observation, observationResourceId.getResourceId().toString());
        list.add(reference, childEventIdCell);*/

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
        for (Long parentEventId: clinicalEventChildMap.keySet()) {
            ReferenceList list = clinicalEventChildMap.get(parentEventId);
            updateExistingObservationWithNewChildLinks(parentEventId, list, fhirResourceFiler);
        }
    }


    private void updateExistingObservationWithNewChildLinks(Long parentEventId,
                                                            ReferenceList childResourceRelationships,
                                                            FhirResourceFiler fhirResourceFiler) throws Exception {

        //convert the parent event ID to a UUID
        //TODO - apply ID mapping or not, but this needs thinking through
        if (true) {
            throw new RuntimeException("Fix code");
        }
        /*CsvCell dummyCell = new CsvCell(-1, -1, "" + parentEventId, null);
        ResourceId observationResourceId = BasisTransformer.getOrCreateObservationResourceId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, dummyCell);
        Observation observation = (Observation)retrieveResource(ResourceType.Observation, observationResourceId.getResourceId());
        if (observation == null) {
            return;
        }

        ResourceBuilderBase resourceBuilder = new ObservationBuilder(observation);

        boolean changed = false;

        for (int i=0; i<childResourceRelationships.size(); i++) {
            Reference reference = childResourceRelationships.getReference(i);
            CsvCell[] sourceCells = childResourceRelationships.getSourceCells(i);

            ObservationBuilder observationBuilder = (ObservationBuilder)resourceBuilder;
            if (observationBuilder.addChildObservation(reference, sourceCells)) {
                changed = true;
            }
        }

        if (changed) {
            //make sure to pass in the parameter to bypass ID mapping, since this resource has already been done
            fhirResourceFiler.savePatientResource(null, false, resourceBuilder);
        }*/
    }

    public void saveInternalId(String idType, String sourceId, String destinationId) throws Exception {
        String cacheKey = idType + "|" + sourceId;

        internalIdDal.upsertRecord(serviceId, idType, sourceId, destinationId);

        if (internalIdMapCache.containsKey(cacheKey)) {
            internalIdMapCache.replace(cacheKey, destinationId);
        } else {
            internalIdMapCache.put(cacheKey, destinationId);
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


}
