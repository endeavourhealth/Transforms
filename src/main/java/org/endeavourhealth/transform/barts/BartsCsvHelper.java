package org.endeavourhealth.transform.barts;

import com.google.common.base.Strings;
import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.hl7receiver.Hl7ResourceIdDalI;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.CernerCodeValueRefDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.emis.csv.helpers.ReferenceList;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class BartsCsvHelper implements HasServiceSystemAndExchangeIdI {
    private static final Logger LOG = LoggerFactory.getLogger(BartsCsvHelper.class);

    public static final String CODE_TYPE_SNOMED = "SNOMED";
    public static final String CODE_TYPE_ICD_10 = "ICD10WHO";
    public static final String CODE_TYPE_OPCS_4 = "OPCS4";

    //the daily files have dates formatted different to the bulks, so we need to support both
    private static SimpleDateFormat DATE_FORMAT_DAILY = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    private static SimpleDateFormat DATE_FORMAT_BULK = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
    private static Date cachedEndOfTime = null;
    private static Date cachedStartOfTime = null;

    private CernerCodeValueRefDalI cernerCodeValueRefDalI = DalProvider.factoryCernerCodeValueRefDal();
    private Hl7ResourceIdDalI hl7ReceiverDal = DalProvider.factoryHL7ResourceDal();
    private InternalIdDalI internalIdDal = DalProvider.factoryInternalIdDal();
    private ResourceDalI resourceRepository = DalProvider.factoryResourceDal();

    private HashMap<String, CernerCodeValueRef> cernerCodes = new HashMap<>();
    //private HashMap<String, ResourceId> resourceIds = new HashMap<>();
    //private Map<String, UUID> locationIdMap = new HashMap<String, UUID>();
    private HashMap<String, String> internalIdMapCache = new HashMap<>();
    private String cachedBartsOrgRefId = null;

    private Map<Long, String> encounterIdToPersonIdMap = new HashMap<>();
    /*private Map<Long, UUID> encounterIdToEnconterResourceMap = new HashMap<>();
    private Map<Long, UUID> encounterIdToPatientResourceMap = new HashMap<>();
    private Map<Long, UUID> personIdToPatientResourceMap = new HashMap<>();*/
    private Map<Long, ReferenceList> clinicalEventChildMap = new HashMap<>();
    private Map<Long, String> patientRelationshipTypeMap = new HashMap<>();

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

    public String getHl7ReceiverScope() {
        return BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE;
    }

    public String getHl7ReceiverGlobalScope() {
        return "G";
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

    public CernerCodeValueRef lookupCodeRef(Long codeSet, CsvCell codeCell) throws Exception {

        String code = codeCell.getString();
        String codeLookup = code.toString() + "|" + serviceId.toString();

        if (code.equals("0")) {
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
        if (code.equals("0")) {
            cernerCodeFromDB = cernerCodeValueRefDalI.getCodeFromCodeSet(
                    codeSet, code, serviceId);
        } else {
            cernerCodeFromDB = cernerCodeValueRefDalI.getCodeWithoutCodeSet(code, serviceId);
        }

        //TODO - trying to track errors so don't return null from here, but remove once we no longer want to process missing codes
        if (cernerCodeFromDB == null) {
            TransformWarnings.log(LOG, this, "Failed to find Cerner CVREF record for code {} and code set {}", codeCell.getString(), codeSet);
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

    /*public static ResourceId getResourceIdFromCache(String resourceIdLookup) {
        return resourceIds.get(resourceIdLookup);
    }

    public static void addResourceIdToCache(ResourceId resourceId) {
        String resourceIdLookup = resourceId.getScopeId()
                + "|" + resourceId.getResourceType()
                + "|" + resourceId.getUniqueId() ;
        resourceIds.put(resourceIdLookup, resourceId);
    }*/

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

            Encounter encounter = (Encounter)retrieveResourceForLocalId(ResourceType.Encounter, encounterIdCell);
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

    /*public void cacheEncounterIds(CsvCell encounterIdCell, Encounter encounter) {
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

        Encounter fhirEncounter = (Encounter)retrieveResourceForUuid(ResourceType.Encounter, encounterResourceId.getResourceId());
        if (fhirEncounter == null) {
            //if encounter has been deleted, add nulls to the map so we don't keep hitting the DB
            encounterIdToEnconterResourceMap.put(encounterId, null);
            encounterIdToPatientResourceMap.put(encounterId, null);
            return;
        }

        cacheEncounterIds(encounterIdCell, fhirEncounter);
    }*/


    /*public UUID findPatientIdFromPersonId(CsvCell personIdCell) throws Exception {

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
    }*/

    public void cacheParentChildClinicalEventLink(CsvCell childEventIdCell, CsvCell parentEventIdCell) throws Exception {
        Long parentEventId = parentEventIdCell.getLong();
        ReferenceList list = clinicalEventChildMap.get(parentEventId);
        if (list == null) {
            list = new ReferenceList();
            clinicalEventChildMap.put(parentEventId, list);
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
        for (Long parentEventId: clinicalEventChildMap.keySet()) {
            ReferenceList list = clinicalEventChildMap.get(parentEventId);
            updateExistingObservationWithNewChildLinks(parentEventId, list, fhirResourceFiler);
        }
    }


    private void updateExistingObservationWithNewChildLinks(Long parentEventId,
                                                            ReferenceList childResourceRelationships,
                                                            FhirResourceFiler fhirResourceFiler) throws Exception {

        Observation observation = (Observation)retrieveResourceForLocalId(ResourceType.Observation, parentEventId.toString());
        if (observation == null) {
            return;
        }

        ObservationBuilder observationBuilder = new ObservationBuilder(observation);

        boolean changed = false;

        for (int i=0; i<childResourceRelationships.size(); i++) {
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
    public UUID createResourceIdOrCopyFromHl7Receiver(ResourceType resourceType, String localUniqueId, String hl7ReceiverUniqueId, String hl7ReceiverScope) throws Exception{

        //check our normal ID -> UUID mapping table
        UUID existingResourceId = IdHelper.getEdsResourceId(serviceId, resourceType, localUniqueId);
        if (existingResourceId != null) {
            return existingResourceId;
        }

        //if no local mapping, check the HL7Receiver DB for the mapping
        ResourceId existingHl7Mapping = hl7ReceiverDal.getResourceId(hl7ReceiverScope, resourceType.toString(), hl7ReceiverUniqueId);
        if (existingHl7Mapping != null) {
            //if the HL7Receiver has a mapped UUID, then store in our local mapping table
            existingResourceId = existingHl7Mapping.getResourceId();
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

        hl7ReceiverDal.saveResourceId(existingHl7Mapping);

        return existingResourceId;

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

        String dateString = cell.getString();
        // try to avoid expected ParseExceptions by guessing the correct dateFormat
        if (dateString.contains(".")) {
            try {
                return DATE_FORMAT_BULK.parse(dateString);
            } catch (ParseException ex) {
                return DATE_FORMAT_DAILY.parse(dateString);
            }

        } else {
            try {
                return DATE_FORMAT_DAILY.parse(dateString);
            } catch (ParseException ex) {
                return DATE_FORMAT_BULK.parse(dateString);
            }
        }
    }

    public void cachePatientRelationshipType(CsvCell relationshipIdCell, String typeDesc) {
        Long relationshipId = relationshipIdCell.getLong();
        patientRelationshipTypeMap.put(relationshipId, typeDesc);
    }

    public String getPatientRelationshipType(CsvCell relationshipIdCell, CsvCell personIdCell) throws Exception {
        //check the cache first, which will contain the new relationships from this Exchange
        Long relationshipId = relationshipIdCell.getLong();
        String ret = patientRelationshipTypeMap.get(relationshipId);
        if (ret != null) {
            return ret;
        }

        //if not in the cache, check the DB, which means retrieving the patient
        Patient patient = (Patient)retrieveResourceForLocalId(ResourceType.Patient, personIdCell);
        if (patient == null) {
            return null;
        }

        PatientBuilder patientBuilder = new PatientBuilder(patient);
        Patient.ContactComponent relationship = PatientContactBuilder.findExistingContactPoint(patientBuilder, relationshipIdCell.getString());
        if (relationship == null) {
            return null;
        }

        if (!relationship.hasRelationship()) {
            return null;
        }

        CodeableConcept codeableConcept = relationship.getRelationship().get(0);
        return codeableConcept.getText();
    }

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
}
