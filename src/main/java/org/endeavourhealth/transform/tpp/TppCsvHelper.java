package org.endeavourhealth.transform.tpp;

import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherCommon.TppCtv3HierarchyRefDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.TppCtv3LookupDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.TppImmunisationContentDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.TppMultiLexToCtv3MapDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppCtv3Lookup;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppImmunisationContent;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMultiLexToCtv3Map;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.TppConfigListOptionDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.TppMappingRefDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.TppConfigListOption;
import org.endeavourhealth.core.database.dal.publisherTransform.models.TppMappingRef;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.ResourceBuilderBase;
import org.endeavourhealth.transform.emis.csv.helpers.ReferenceList;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class TppCsvHelper implements HasServiceSystemAndExchangeIdI {
    private static final Logger LOG = LoggerFactory.getLogger(TppCsvHelper.class);

    private static final String ID_DELIMITER = ":";

    private static final String ALLERGIC_DISORDER = "Xa1pQ";

    private static final ParserPool PARSER_POOL = new ParserPool();

    private static TppMappingRefDalI tppMappingRefDalI = DalProvider.factoryTppMappingRefDal();
    private static HashMap<String, TppMappingRef> tppMappingRefs = new HashMap<>();

    private static TppConfigListOptionDalI tppConfigListOptionDalI = DalProvider.factoryTppConfigListOptionDal();
    private static HashMap<String, TppConfigListOption> tppConfigListOptions = new HashMap<>();

    private static TppImmunisationContentDalI tppImmunisationContentDalI = DalProvider.factoryTppImmunisationContentDal();
    private static HashMap<String, TppImmunisationContent> tppImmunisationContents = new HashMap<>();

    private static InternalIdDalI internalIdDalI = DalProvider.factoryInternalIdDal();
    private static HashMap<String, String> internalIdMapCache = new HashMap<>();

    private static TppMultiLexToCtv3MapDalI multiLexToCTV3MapDalI = DalProvider.factoryTppMultiLexToCtv3MapDal();
    private static HashMap<String, TppMultiLexToCtv3Map> multiLexToCTV3Map = new HashMap<>();

    private static TppCtv3HierarchyRefDalI ctv3HierarchyRefDalI = DalProvider.factoryTppCtv3HierarchyRefDal();

    private static TppCtv3LookupDalI tppCtv3LookupRefDal = DalProvider.factoryTppCtv3LookupDal();
    private static HashMap<String, TppCtv3Lookup> tppCtv3Lookups = new HashMap<>();

    private Map<String, ReferenceList> consultationNewChildMap = new HashMap<>();
    private Map<String, ReferenceList> consultationExistingChildMap = new ConcurrentHashMap<>(); //written to by many threads

    private Map<String, ReferenceList> encounterAppointmentOrVisitMap = new HashMap<>();

    private Map<String, Map.Entry<Date, CsvCell>> medicalRecordStatusMap = new HashMap<>();

    private Map<String, String> problemReadCodes = new HashMap<>();
    private Map<String, String> allergyReadCodes = new HashMap<>();

    private final UUID serviceId;
    private final UUID systemId;
    private final UUID exchangeId;
    private final String dataSharingAgreementGuid;
    private final boolean processPatientData;


    public TppCsvHelper(UUID serviceId, UUID systemId, UUID exchangeId, String dataSharingAgreementGuid, boolean processPatientData) {
        this.serviceId = serviceId;
        this.systemId = systemId;
        this.exchangeId = exchangeId;
        this.dataSharingAgreementGuid = dataSharingAgreementGuid;
        this.processPatientData = processPatientData;
    }

    private ResourceDalI resourceRepository = DalProvider.factoryResourceDal();

    public Reference createOrganisationReference(CsvCell organizationGuid) {
        return ReferenceHelper.createReference(ResourceType.Organization, organizationGuid.getString());
    }

    public Reference createLocationReference(CsvCell locationGuid) {
        return ReferenceHelper.createReference(ResourceType.Location, locationGuid.getString());
    }

    public Reference createPatientReference(CsvCell patientGuid) {
        return ReferenceHelper.createReference(ResourceType.Patient, patientGuid.getString());
    }

    public Reference createPractitionerReference(CsvCell practitionerGuid) {
        return ReferenceHelper.createReference(ResourceType.Practitioner, practitionerGuid.getString());
    }

    public Reference createPractitionerReference(String practitionerGuid) {
        return ReferenceHelper.createReference(ResourceType.Practitioner, practitionerGuid);
    }

    public Reference createScheduleReference(CsvCell scheduleGuid) {
        return ReferenceHelper.createReference(ResourceType.Schedule, scheduleGuid.getString());
    }

    public Reference createSlotReference(CsvCell slotGuid) {
        return ReferenceHelper.createReference(ResourceType.Slot, slotGuid.getString());
    }

    public Reference createConditionReference(CsvCell problemGuid, CsvCell patientGuid) {
        return ReferenceHelper.createReference(ResourceType.Condition, createUniqueId(patientGuid, problemGuid));
    }

    public Reference createMedicationStatementReference(CsvCell medicationStatementGuid, CsvCell patientGuid) {
        return ReferenceHelper.createReference(ResourceType.MedicationStatement, createUniqueId(patientGuid, medicationStatementGuid));
    }

    public Reference createEncounterReference(CsvCell encounterGuid, CsvCell patientGuid) {
        return ReferenceHelper.createReference(ResourceType.Encounter, createUniqueId(patientGuid, encounterGuid));
    }

    public static void setUniqueId(ResourceBuilderBase resourceBuilder, CsvCell patientGuid, CsvCell sourceGuid) {
        String resourceId = createUniqueId(patientGuid, sourceGuid);
        resourceBuilder.setId(resourceId, patientGuid, sourceGuid);
    }

    public static String createUniqueId(CsvCell patientGuid, CsvCell sourceGuid) {
        // uniqueId is literally now the sourceId without the patientId
        return sourceGuid.getString();

//        if (sourceGuid == null) {
//            return patientGuid.getString();
//        } else {
//            return patientGuid.getString() + ID_DELIMITER + sourceGuid.getString();
//        }
    }



    public Resource retrieveResource(String locallyUniqueId, ResourceType resourceType, FhirResourceFiler fhirResourceFiler) throws Exception {

        UUID serviceId = fhirResourceFiler.getServiceId();
        UUID systemId = fhirResourceFiler.getSystemId();

        UUID globallyUniqueId = IdHelper.getEdsResourceId(serviceId, systemId, resourceType, locallyUniqueId);

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
                                                      CsvCell patientGuid,
                                                      CsvCell resourceGuid,
                                                      ResourceType resourceType) {

        if (consultationGuid.isEmpty()) {
            return;
        }

        String consultationLocalUniqueId = createUniqueId(patientGuid, consultationGuid);
        ReferenceList list = consultationNewChildMap.get(consultationLocalUniqueId);
        if (list == null) {
            list = new ReferenceList();
            consultationNewChildMap.put(consultationLocalUniqueId, list);
        }

        String resourceLocalUniqueId = createUniqueId(patientGuid, resourceGuid);
        Reference resourceReference = ReferenceHelper.createReference(resourceType, resourceLocalUniqueId);
        list.add(resourceReference, consultationGuid);
    }

    public ReferenceList getAndRemoveNewConsultationRelationships(String encounterSourceId) {
        return consultationNewChildMap.remove(encounterSourceId);
    }

    public void cacheNewEncounterAppointmentOrVisitMap(CsvCell encounterId,
                                                CsvCell patientGuid,
                                                String resourceGuid,
                                                ResourceType resourceType) {

        if (encounterId.isEmpty()) {
            return;
        }

        String consultationLocalUniqueId = createUniqueId(patientGuid, encounterId);
        ReferenceList list = encounterAppointmentOrVisitMap.get(consultationLocalUniqueId);
        if (list == null) {
            list = new ReferenceList();
            encounterAppointmentOrVisitMap.put(consultationLocalUniqueId, list);
        }

        String resourceLocalUniqueId = resourceGuid;
        Reference resourceReference = ReferenceHelper.createReference(resourceType, resourceLocalUniqueId);
        list.add(resourceReference, encounterId);
    }

    public ReferenceList getAndRemoveEncounterAppointmentOrVisitMap(String encounterSourceId) {
        return encounterAppointmentOrVisitMap.remove(encounterSourceId);
    }

    public void cacheConsultationPreviousLinkedResources(String encounterSourceId, List<Reference> previousReferences) {

        if (previousReferences == null
                || previousReferences.isEmpty()) {
            return;
        }

        ReferenceList obj = new ReferenceList();
        obj.add(previousReferences);

        consultationExistingChildMap.put(encounterSourceId, obj);
    }

    public ReferenceList findConsultationPreviousLinkedResources(String encounterSourceId) {
        return consultationExistingChildMap.remove(encounterSourceId);
    }

    public void cacheProblemObservationGuid(CsvCell patientGuid, CsvCell problemGuid, String readCode) {
        problemReadCodes.put(createUniqueId(patientGuid, problemGuid), readCode);
    }

    public boolean isProblemObservationGuid(CsvCell patientGuid, CsvCell problemGuid) {
        return problemReadCodes.containsKey(createUniqueId(patientGuid, problemGuid));
    }

    public void cacheMedicalRecordStatus(CsvCell patientGuid, Date newStatusDate, CsvCell medicalRecordStatusCell) {

        // Create the unique Id
        String uniquePatientId = createUniqueId(patientGuid, null);

        // Check if we already have a status for this patient
        Map.Entry<Date, CsvCell> statusForPatient = medicalRecordStatusMap.get(patientGuid);

        if (statusForPatient != null) {
            Date existingDate = statusForPatient.getKey();
            // Check if the new status has a data after the existing status
            if (newStatusDate.after(existingDate)) {
                // Overwrite the existing status the the new status
                medicalRecordStatusMap.put(uniquePatientId, new AbstractMap.SimpleEntry(newStatusDate, medicalRecordStatusCell));
            }
        } else {
            medicalRecordStatusMap.put(uniquePatientId, new AbstractMap.SimpleEntry(newStatusDate, medicalRecordStatusCell));
        }
    }

    public CsvCell getAndRemoveMedicalRecordStatus(CsvCell patientGuid) {
        // Create the unique Id
        String uniquePatientId = createUniqueId(patientGuid, null);
        // Find and remove the status entry
        Map.Entry<Date, CsvCell> statusForPatient = medicalRecordStatusMap.remove(patientGuid);
        // return the status
        return statusForPatient.getValue();
    }

    public void cacheAllergyCode(String readCode, String readTerm) {
        allergyReadCodes.put(readCode, readTerm);
    }

    public boolean isAllergyCode(String readCode, String readTerm) throws Exception {

        // check cache first
        if (allergyReadCodes.containsKey(readCode))
            return true;

        // check db and cache if true
        boolean isAllergy
                = ctv3HierarchyRefDalI.isChildCodeUnderParentCode(readCode, ALLERGIC_DISORDER);
        if (isAllergy) {
            cacheAllergyCode(readCode, readTerm);
            return true;
        }

        // otherwise, it's not an allergy code
        return false;
    }

    // Lookup code reference from SRMapping generated db
    public TppMappingRef lookUpTppMappingRef(CsvCell cell, AbstractCsvParser parser) throws Exception {

        Long rowId = cell.getLong();
        String codeLookup = rowId.toString() + "|" + serviceId.toString();

        //Find the code in the cache
        TppMappingRef tppMappingRefFromCache = tppMappingRefs.get(codeLookup);

        // return cached version if exists
        if (tppMappingRefFromCache != null) {
            return tppMappingRefFromCache;
        }

        TppMappingRef tppMappingRefFromDB = tppMappingRefDalI.getMappingFromRowId(rowId, serviceId);
        if (tppMappingRefFromDB == null) {

            TransformWarnings.log(LOG, parser, "TPP mapping reference not found for id: {},  in file: {}, line: {}",
                    rowId, parser.getFilePath(), parser.getCurrentLineNumber());
            return null;
        }

        // Add to the cache
        tppMappingRefs.put(codeLookup, tppMappingRefFromDB);

        return tppMappingRefFromDB;
    }

    // Lookup code reference from SRConfigureListOption generated db
    public TppConfigListOption lookUpTppConfigListOption(CsvCell cell, AbstractCsvParser parser) throws Exception {

        Long rowId = cell.getLong();
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
        String cacheKey = idType + "|" + sourceId;

        internalIdDalI.upsertRecord(serviceId, idType, sourceId, destinationId);

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

        String ret = internalIdDalI.getDestinationId(serviceId, idType, sourceId);

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

    public String getDataSharingAgreementGuid() {
        return dataSharingAgreementGuid;
    }

    public boolean isProcessPatientData() {
        return processPatientData;
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


}