package org.endeavourhealth.transform.adastra;

import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.admin.ServiceDalI;
import org.endeavourhealth.core.database.dal.admin.models.Service;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.transform.adastra.cache.EpisodeOfCareResourceCache;
import org.endeavourhealth.transform.adastra.cache.OrganisationResourceCache;
import org.endeavourhealth.transform.adastra.cache.PatientResourceCache;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.referenceLists.ReferenceList;
import org.endeavourhealth.transform.common.referenceLists.ReferenceListSingleCsvCells;
import org.hl7.fhir.instance.model.DomainResource;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class AdastraCsvHelper {
    private static final String ID_DELIMITER = ":";

    private final UUID serviceId;
    private final UUID systemId;
    private final UUID exchangeId;

    private static final ParserPool PARSER_POOL = new ParserPool();

    private Map<String, ReferenceList> consultationNewChildMap = new HashMap<>();

    private Map<String, CsvCell> casePatientMap = new HashMap<>();
    private Map<String, CsvCell> consultationDateMap = new HashMap<>();
    private Map<String, CsvCell> caseCaseNoMap = new HashMap<>();
    private Map<String, String> caseOutcomeMap = new HashMap<>();

    private PatientResourceCache patientCache = new PatientResourceCache();
    private EpisodeOfCareResourceCache episodeOfCareCache = new EpisodeOfCareResourceCache();
    private OrganisationResourceCache organisationCache = new OrganisationResourceCache();

    private ResourceDalI resourceRepository = DalProvider.factoryResourceDal();
    private ServiceDalI serviceRepository = DalProvider.factoryServiceDal();

    public AdastraCsvHelper(UUID serviceId, UUID systemId, UUID exchangeId) {
        this.serviceId = serviceId;
        this.systemId = systemId;
        this.exchangeId = exchangeId;
    }

    /**
     * to ensure globally unique IDs for all resources, a new ID is created
     * from the patientGuid and sourceGuid (e.g. observationGuid)
     */
    public static String createUniqueId(String patientGuid, String sourceGuid) {
        if (sourceGuid == null) {
            return patientGuid;
        } else {
            return patientGuid + ID_DELIMITER + sourceGuid;
        }
    }

    public Resource retrieveResource(String locallyUniqueId, ResourceType resourceType, FhirResourceFiler fhirResourceFiler) throws Exception {

        UUID serviceId = fhirResourceFiler.getServiceId();
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

    // if the resource is already filed and has been retrieved from the DB, the sourceId will differ from the
    // saved (mapped) resource Id
    public boolean isResourceIdMapped (String sourceId, DomainResource resource) {
        return !resource.getId().equals(sourceId);
    }

    public Service getService (UUID id) throws Exception {
        return serviceRepository.getById(id);
    }

    public PatientResourceCache getPatientCache() { return patientCache; }

    public OrganisationResourceCache getOrganisationCache() { return organisationCache; }

    public EpisodeOfCareResourceCache getEpisodeOfCareCache() { return episodeOfCareCache; }

    public Reference createEncounterReference(CsvCell encounterGuid) {
        return ReferenceHelper.createReference(ResourceType.Encounter, encounterGuid.getString());
    }

    public Reference createPatientReference(CsvCell patientGuid) {
        return ReferenceHelper.createReference(ResourceType.Patient, patientGuid.getString());
    }

    public Reference createEpisodeReference(CsvCell episodeGuid) {
        return ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeGuid.getString());
    }

    public Reference createOrganisationReference(String organizationGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Organization, organizationGuid);
    }

    public void cacheCaseOutcome(String caseId, String outcomeText)  {
        caseOutcomeMap.put(caseId, outcomeText);
    }

    public String getCaseOutcome(String caseId)  {
        return caseOutcomeMap.get(caseId);
    }

    public void cacheCasePatient(String caseId, CsvCell patientIdCell) {
        casePatientMap.put(caseId, patientIdCell);
    }

    public CsvCell findCasePatient(String caseId) {
        return casePatientMap.get(caseId);
    }

    public void cacheCaseCaseNo(String caseId, CsvCell caseNoCell) {
        caseCaseNoMap.put(caseId, caseNoCell);
    }

    public CsvCell findCaseCaseNo(String caseId) {
        return caseCaseNoMap.get(caseId);
    }

    public void cacheConsultationDate(String consultationId, CsvCell consultationDateCell) {
        consultationDateMap.put(consultationId, consultationDateCell);
    }

    public CsvCell findConsultationDateTime(String consultationId) {
        return consultationDateMap.get(consultationId);
    }

    public void cacheNewConsultationChildRelationship(CsvCell consultationIDCell,
                                                      String resourceGuid,
                                                      ResourceType resourceType) {

        if (consultationIDCell.isEmpty()) {
            return;
        }

        //String consultationLocalUniqueId = createUniqueId(patientGuid, consultationIDCell.getString());
        String consultationLocalUniqueId = consultationIDCell.getString();
        ReferenceList list = consultationNewChildMap.get(consultationLocalUniqueId);
        if (list == null) {
            //we know there will be only one CsvCells, so use this reference list class to save memory
            list = new ReferenceListSingleCsvCells();
            //list = new ReferenceList();
            consultationNewChildMap.put(consultationLocalUniqueId, list);
        }

        //String resourceLocalUniqueId = createUniqueId(patientGuid, resourceGuid);
        String resourceLocalUniqueId = resourceGuid;
        Reference resourceReference = ReferenceHelper.createReference(resourceType, resourceLocalUniqueId);
        list.add(resourceReference, consultationIDCell);
    }

    public ReferenceList getAndRemoveNewConsultationRelationships(String encounterSourceId) {
        return consultationNewChildMap.remove(encounterSourceId);
    }
}
