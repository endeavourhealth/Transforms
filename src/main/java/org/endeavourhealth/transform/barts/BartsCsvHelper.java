package org.endeavourhealth.transform.barts;

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
import org.endeavourhealth.transform.common.BasisTransformer;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class BartsCsvHelper {
    private static final Logger LOG = LoggerFactory.getLogger(BartsCsvHelper.class);

    public static final String CODE_TYPE_SNOMED = "SNOMED";
    public static final String CODE_TYPE_ICD_10 = "ICD10WHO";

    private static CernerCodeValueRefDalI cernerCodeValueRefDalI = DalProvider.factoryCernerCodeValueRefDal();
    private static HashMap<String, CernerCodeValueRef> cernerCodes = new HashMap<>();
    private static HashMap<String, ResourceId> resourceIds = new HashMap<>();

    //non-static caches
    private Map<Long, UUID> encounterIdToEnconterResourceMap = new HashMap<>();
    private Map<Long, UUID> encounterIdToPatientResourceMap = new HashMap<>();
    private Map<Long, UUID> personIdToPatientResourceMap = new HashMap<>();

    private InternalIdDalI internalIdDal = DalProvider.factoryInternalIdDal();
    private ResourceDalI resourceRepository = DalProvider.factoryResourceDal();
    private UUID serviceId = null;
    private UUID systemId = null;
    private String primaryOrgHL7OrgOID = null;

    public BartsCsvHelper(UUID serviceId, UUID systemId, String primaryOrgHL7OrgOID) {
        this.serviceId = serviceId;
        this.systemId = systemId;
        this.primaryOrgHL7OrgOID = primaryOrgHL7OrgOID;
    }

    public String getPrimaryOrgHL7OrgOID() {
        return primaryOrgHL7OrgOID;
    }

    public UUID getServiceId() {
        return serviceId;
    }

    public UUID getSystemId() {
        return systemId;
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

    public static CernerCodeValueRef lookUpCernerCodeFromCodeSet(Long codeSet, Long code, UUID serviceId) throws Exception {

        String codeLookup = codeSet.toString() + "|" + code.toString() + "|" + serviceId.toString();

        //Find the code in the cache
        CernerCodeValueRef cernerCodeFromCache =  cernerCodes.get(codeLookup);

        // return cached version if exists
        if (cernerCodeFromCache != null) {
            return cernerCodeFromCache;
        }

        // get code from DB
        CernerCodeValueRef cernerCodeFromDB = cernerCodeValueRefDalI.getCodeFromCodeSet(
                codeSet, code, serviceId);

        //TODO - trying to track errors so don't return null from here, but remove once we no longer want to process missing codes
        if (cernerCodeFromDB == null) {
            return new CernerCodeValueRef();
        }

        // Add to the cache
        cernerCodes.put(codeLookup, cernerCodeFromDB);

        return cernerCodeFromDB;
    }

    public static ResourceId getResourceIdFromCache (String resourceIdLookup) {
        return resourceIds.get(resourceIdLookup);
    }

    public static void addResourceIdToCache (ResourceId resourceId) {
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

        ResourceId encounterResourceId = BasisTransformer.getEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, encounterId.toString());
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

            String mrn = internalIdDal.getDestinationId(serviceId, InternalIdMap.TYPE_MILLENNIUM_PERSON_ID_TO_MRN, personIdCell.getString());
            if (mrn == null) {
                //if we've never received the patient, we won't have a map to its MRN but don't add to the map so if it is created, we'll start working
                return null;

            } else {

                ResourceId resourceId = BasisTransformer.getPatientResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, primaryOrgHL7OrgOID, mrn);
                if (resourceId == null) {
                    //if we've got the MRN mapping, but haven't actually assigned an ID for it, do so now
                    resourceId = BasisTransformer.createPatientResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, primaryOrgHL7OrgOID, mrn);

                }

                UUID patientId = resourceId.getResourceId();
                personIdToPatientResourceMap.put(personId, patientId);
            }
        }

        return personIdToPatientResourceMap.get(personIdCell);
    }
}
