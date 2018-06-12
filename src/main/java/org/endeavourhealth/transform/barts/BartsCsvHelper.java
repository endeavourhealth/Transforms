package org.endeavourhealth.transform.barts;

import com.google.common.base.Strings;
import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.CernerCodeValueRefDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.ObservationBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ResourceBuilderBase;
import org.endeavourhealth.transform.emis.csv.helpers.ReferenceList;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

import static org.endeavourhealth.transform.common.BasisTransformer.getLocationResourceId;

public class BartsCsvHelper implements HasServiceSystemAndExchangeIdI {
    private static final Logger LOG = LoggerFactory.getLogger(BartsCsvHelper.class);

    public static final String CODE_TYPE_SNOMED = "SNOMED";
    public static final String CODE_TYPE_ICD_10 = "ICD10WHO";
    public static final String CODE_TYPE_OPCS_4 = "OPCS4";

    private static CernerCodeValueRefDalI cernerCodeValueRefDalI = DalProvider.factoryCernerCodeValueRefDal();
    private static HashMap<String, CernerCodeValueRef> cernerCodes = new HashMap<>();
    private static HashMap<String, ResourceId> resourceIds = new HashMap<>();
    private static Map<String, UUID> locationIdMap = new HashMap<String, UUID>();
    private static HashMap<String, String> internalIdMapCache = new HashMap<>();
    private static Date cachedEndOfTime = null;

    //non-static caches
    private Map<Long, UUID> encounterIdToEnconterResourceMap = new HashMap<>();
    private Map<Long, UUID> encounterIdToPatientResourceMap = new HashMap<>();
    private Map<Long, UUID> personIdToPatientResourceMap = new HashMap<>();
    private Map<Long, ReferenceList> clinicalEventChildMap = new HashMap<>();

    private InternalIdDalI internalIdDal = DalProvider.factoryInternalIdDal();
    private ResourceDalI resourceRepository = DalProvider.factoryResourceDal();
    private UUID serviceId = null;
    private UUID systemId = null;
    private UUID exchangeId = null;
    private String primaryOrgHL7OrgOID = null;
    private String version = null;



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


    public String getVersion() {
        return version;
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

    public List<Resource> retrieveResourceByPatient(UUID patientId) throws Exception {
        List<Resource> ret = null;
        List<ResourceWrapper> resourceList = resourceRepository.getResourcesByPatient(serviceId, systemId, patientId);
        for (ResourceWrapper rw : resourceList) {
            if (ret == null) {
                ret = new ArrayList<>();
            }
            String json = rw.getResourceData();
            ret.add(ParserPool.getInstance().parse(json));
        }
        return ret;
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

    public Reference createPractitionerReference(String practitionerGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Practitioner, practitionerGuid);
    }

    /*public Reference createPractitionerReference(CsvCell practitionerIdCell) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Practitioner, practitionerIdCell.getString());
    }*/

    public String getProcedureOrDiagnosisConceptCodeType(CsvCell cell) {
        if (cell.isEmpty()) {
            return null;
        }
        String conceptCodeIdentifier = cell.getString();
        int index = conceptCodeIdentifier.indexOf('!');
        if (index > -1) {
            String ret = conceptCodeIdentifier.substring(0,index);
            if (ret.equals(CODE_TYPE_SNOMED)
                    || ret.equals(CODE_TYPE_ICD_10)
                    || ret.equalsIgnoreCase(CODE_TYPE_OPCS_4)) {
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

    public void saveLocationUUIDToCache(String locationId, UUID resourceUUID) throws Exception {
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
    }

    public CernerCodeValueRef lookupCodeRef(Long codeSet, CsvCell codeCell) throws Exception {

        String code = codeCell.getString();
        String codeLookup = code.toString() + "|" + serviceId.toString();

        if (code.equals(0)) {
            codeLookup = codeSet + "|" + codeLookup;
        }

        //Find the code in the cache
        CernerCodeValueRef cernerCodeFromCache =  cernerCodes.get(codeLookup);

        // return cached version if exists
        if (cernerCodeFromCache != null) {
            return cernerCodeFromCache;
        }

        CernerCodeValueRef cernerCodeFromDB = null;

        // get code from DB (special case for a code of 0 as that is duplicated)
        if (code.equals(0)) {
            cernerCodeFromDB = cernerCodeValueRefDalI.getCodeFromCodeSet(
                    codeSet, code, serviceId);
        } else {
            cernerCodeFromDB = cernerCodeValueRefDalI.getCodeWithoutCodeSet(code, serviceId);
        }

        //TODO - trying to track errors so don't return null from here, but remove once we no longer want to process missing codes
        if (cernerCodeFromDB == null) {
           // return new CernerCodeValueRef();
            return null;
        }

        //seem to have whitespace around some of the fields. As a temporary fix, trim them here
        if (!Strings.isNullOrEmpty(cernerCodeFromDB.getAliasNhsCdAlias())) {
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
        }

        // Add to the cache
        cernerCodes.put(codeLookup, cernerCodeFromDB);

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


    public UUID findEncounterResourceIdFromEncounterId(CsvCell encounterIdCell) throws Exception {
        ensureEncounterIdsAreCached(encounterIdCell);
        Long encounterId = encounterIdCell.getLong();
        return encounterIdToEnconterResourceMap.get(encounterId);
    }

    public UUID findPatientIdFromEncounterId(CsvCell encounterIdCell) throws Exception {
        ensureEncounterIdsAreCached(encounterIdCell);
        Long encounterId = encounterIdCell.getLong();
        return encounterIdToPatientResourceMap.get(encounterId);
    }

    public void cacheEncounterIds(CsvCell encounterIdCell, Encounter encounter) {
        Long encounterId = encounterIdCell.getLong();

        String id = encounter.getId();
        UUID encounterUuid = UUID.fromString(id);
        encounterIdToEnconterResourceMap.put(encounterId, encounterUuid);

        Reference patientReference = encounter.getPatient();
        ReferenceComponents comps = ReferenceHelper.getReferenceComponents(patientReference);
        UUID patientUuid = UUID.fromString(comps.getId());
        encounterIdToPatientResourceMap.put(encounterId, patientUuid);
    }

    private void ensureEncounterIdsAreCached(CsvCell encounterIdCell) throws Exception {

        Long encounterId = encounterIdCell.getLong();

        //if already cached, return
        if (encounterIdToEnconterResourceMap.containsKey(encounterId)) {
            return;
        }

        ResourceId encounterResourceId = BasisTransformer.getEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, encounterIdCell.getString());
        if (encounterResourceId == null) {
            //add nulls to the map so we don't keep hitting the DB
            encounterIdToEnconterResourceMap.put(encounterId, null);
            encounterIdToPatientResourceMap.put(encounterId, null);
            return;
        }

        Encounter fhirEncounter = (Encounter)retrieveResource(ResourceType.Encounter, encounterResourceId.getResourceId());
        if (fhirEncounter == null) {
            //if encounter has been deleted, add nulls to the map so we don't keep hitting the DB
            encounterIdToEnconterResourceMap.put(encounterId, null);
            encounterIdToPatientResourceMap.put(encounterId, null);
            return;
        }

        cacheEncounterIds(encounterIdCell, fhirEncounter);
    }


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

                ResourceId resourceId = BasisTransformer.getPatientResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, primaryOrgHL7OrgOID, mrn);
                if (resourceId == null) {
                    //if we've got the MRN mapping, but haven't actually assigned an ID for it, do so now
                    resourceId = BasisTransformer.createPatientResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, primaryOrgHL7OrgOID, mrn);
                    //LOG.trace("Created new resource ID " + resourceId.getResourceId() + " for person ID " + personIdCell.getString());
                }

                UUID patientId = resourceId.getResourceId();
                personIdToPatientResourceMap.put(personId, patientId);
                //LOG.trace("Added patient ID " + resourceId.getResourceId() + " to cache " + personIdCell.getString());
            }
        }

        return personIdToPatientResourceMap.get(personId);
    }

    public void cacheParentChildClinicalEventLink(CsvCell childEventIdCell, CsvCell parentEventIdCell) throws Exception {
        Long parentEventId = parentEventIdCell.getLong();
        ReferenceList list = clinicalEventChildMap.get(parentEventId);
        if (list == null) {
            list = new ReferenceList();
            clinicalEventChildMap.put(parentEventId, list);
        }

        //we need to map the child ID to a Discovery UUID
        ResourceId observationResourceId = BasisTransformer.getOrCreateObservationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, childEventIdCell);
        Reference reference = ReferenceHelper.createReference(ResourceType.Observation, observationResourceId.getResourceId().toString());
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
        for (Long parentEventId: clinicalEventChildMap.keySet()) {
            ReferenceList list = clinicalEventChildMap.get(parentEventId);
            updateExistingObservationWithNewChildLinks(parentEventId, list, fhirResourceFiler);
        }
    }


    private void updateExistingObservationWithNewChildLinks(Long parentEventId,
                                                            ReferenceList childResourceRelationships,
                                                            FhirResourceFiler fhirResourceFiler) throws Exception {

        //convert the parent event ID to a UUID
        CsvCell dummyCell = new CsvCell(-1, -1, "" + parentEventId, null);
        ResourceId observationResourceId = BasisTransformer.getOrCreateObservationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, dummyCell);
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

        Date d = dateCell.getDate();
        if (d.equals(cachedEndOfTime)) {
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

}
