package org.endeavourhealth.transform.vision;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherCommon.VisionCodeDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisClinicalCode;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.referenceLists.ReferenceList;
import org.endeavourhealth.transform.common.referenceLists.ReferenceListNoCsvCells;
import org.endeavourhealth.transform.common.referenceLists.ReferenceListSingleCsvCells;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCodeHelper;
import org.endeavourhealth.transform.vision.helpers.VisionCodeHelper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class VisionCsvHelper implements HasServiceSystemAndExchangeIdI {
    private static final Logger LOG = LoggerFactory.getLogger(VisionCsvHelper.class);

    private static final String ID_DELIMITER = ":";
    private static final String CONTAINED_LIST_ID = "Items";

    private static final ParserPool PARSER_POOL = new ParserPool();

    private final UUID serviceId;
    private final UUID systemId;
    private final UUID exchangeId;
    private ResourceDalI resourceRepository = DalProvider.factoryResourceDal();


    //some resources are referred to by others, so we cache them here for when we need them
    private Map<String, List<String>> observationChildMap = new HashMap<>();
    private Map<String, ReferenceList> newProblemChildren = new HashMap<>();
    private Map<String, ReferenceList> problemPreviousLinkedResources = new ConcurrentHashMap<>(); //written to by many threads
    private Map<String, ReferenceList> consultationNewChildMap = new HashMap<>();
    private Map<String, ReferenceList> consultationExistingChildMap = new ConcurrentHashMap<>(); //written to by many threads
    /*private Map<String, DateType> drugRecordLastIssueDateMap = new HashMap<>();
    private Map<String, DateType> drugRecordFirstIssueDateMap = new HashMap<>();*/
    private Map<StringMemorySaver, DateAndEthnicityCategory> ethnicityMap = new HashMap<>();
    //private Set<String> problemReadCodes = new HashSet<>();
    //private Set<String> drugRecords = new HashSet<>();
    private Map<String, String> latestEpisodeStartDateCache = new HashMap<>();
    private Map<CodeAndTerm, AtomicInteger> hmRead2TermMap = new HashMap<>();
    private Map<String, SnomedConceptAndDate> hmRead2toSnomedMap = new HashMap<>();
    private ThreadPool utilityThreadPool = null;

    public VisionCsvHelper(UUID serviceId, UUID systemId, UUID exchangeId) {
        this.serviceId = serviceId;
        this.systemId = systemId;
        this.exchangeId = exchangeId;
    }

    /**
     * to ensure globally unique IDs for all resources, a new ID is created
     * from the patientGuid and sourceGuid (e.g. observationGuid)
     */
    public static String createUniqueId(CsvCell patientGuid, CsvCell sourceGuid) {
        if (sourceGuid == null) {
            return patientGuid.getString();
        } else {
            return patientGuid.getString() + ID_DELIMITER + sourceGuid.getString();
        }
    }
    public static String createUniqueId(String patientGuid, String sourceGuid) {
        if (sourceGuid == null) {
            return patientGuid;
        } else {
            return patientGuid + ID_DELIMITER + sourceGuid;
        }
    }

    private static String getPatientGuidFromUniqueId(String uniqueId) {
        String[] toks = uniqueId.split(ID_DELIMITER);
        if (toks.length == 1
                || toks.length == 2) {
            return toks[0];
        } else {
            throw new IllegalArgumentException("Invalid unique ID string [" + uniqueId + "] - expect one or two tokens delimited with " + ID_DELIMITER);
        }
    }

    public static void setUniqueId(ResourceBuilderBase resourceBuilder, CsvCell patientGuid, CsvCell sourceGuid) {
        String resourceId = createUniqueId(patientGuid, sourceGuid);
        resourceBuilder.setId(resourceId, patientGuid, sourceGuid);
    }

    /**
     * admin-type resources just use the Vision CSV GUID as their reference
     */
    public static Reference createLocationReference(String locationGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Location, locationGuid);
    }
    public static Reference createOrganisationReference(String organizationGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Organization, organizationGuid);
    }
    public static Reference createPractitionerReference(String practitionerGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Practitioner, practitionerGuid);
    }

    /**
     * patient-type resources must include the patient GUID are part of the unique ID in the reference
     * because the EMIS GUIDs for things like Obs are only unique within that patient record itself
     */
    public static Reference createPatientReference(CsvCell patientGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Patient, createUniqueId(patientGuid, null));
    }

    public Reference createConditionReference(String problemGuid, String patientGuid) {
        if (problemGuid.isEmpty()) {
            throw new IllegalArgumentException("Missing problemGuid");
        }
        return ReferenceHelper.createReference(ResourceType.Condition, createUniqueId(patientGuid, problemGuid));
    }

    public Reference createEncounterReference(String encounterGuid, String patientGuid) throws Exception {
        if (encounterGuid.isEmpty()) {
            throw new IllegalArgumentException("Missing Encounter ID");
        }
        return ReferenceHelper.createReference(ResourceType.Encounter, createUniqueId(patientGuid, encounterGuid));
    }
    public Reference createObservationReference(String observationGuid, String patientGuid) throws Exception {
        if (observationGuid.isEmpty()) {
            throw new IllegalArgumentException("Missing observationGuid");
        }
        return ReferenceHelper.createReference(ResourceType.Observation, createUniqueId(patientGuid, observationGuid));
    }
    public Reference createMedicationStatementReference(String medicationStatementGuid, String patientGuid) throws Exception {
        if (medicationStatementGuid.isEmpty()) {
            throw new IllegalArgumentException("Missing MedicationStatement ID");
        }
        return ReferenceHelper.createReference(ResourceType.MedicationStatement, createUniqueId(patientGuid, medicationStatementGuid));
    }

    public List<String> getAndRemoveObservationParentRelationships(CsvCell parentObservationGuid, CsvCell patientGuid) {
        return observationChildMap.remove(createUniqueId(patientGuid, parentObservationGuid));
    }

    public boolean hasChildObservations(CsvCell parentObservationGuid, CsvCell patientGuid) {
        return observationChildMap.containsKey(createUniqueId(patientGuid, parentObservationGuid));
    }

    public void cacheObservationParentRelationship(CsvCell parentObservationGuid, CsvCell patientGuid, CsvCell observationGuid) {

        List<String> list = observationChildMap.get(createUniqueId(patientGuid, parentObservationGuid));
        if (list == null) {
            list = new ArrayList<>();
            observationChildMap.put(createUniqueId(patientGuid, parentObservationGuid), list);
        }
        list.add(ReferenceHelper.createResourceReference(ResourceType.Observation, createUniqueId(patientGuid, observationGuid)));
    }


    public Resource retrieveResource(String locallyUniqueId, ResourceType resourceType) throws Exception {

        UUID globallyUniqueId = IdHelper.getEdsResourceId(getServiceId(), resourceType, locallyUniqueId);
        if (globallyUniqueId == null) {
            return null;
        }

        UUID serviceId = getServiceId();
        ResourceWrapper resourceHistory = resourceRepository.getCurrentVersion(serviceId, resourceType.toString(), globallyUniqueId);
        if (resourceHistory == null) {
            return null;
        }

        if (resourceHistory.isDeleted()) {
            return null;
        }

        String json = resourceHistory.getResourceData();
        return PARSER_POOL.parse(json);
    }

    public List<Resource> retrieveAllResourcesForPatient(String patientGuid, FhirResourceFiler fhirResourceFiler) throws Exception {

        UUID edsPatientId = IdHelper.getEdsResourceId(fhirResourceFiler.getServiceId(), ResourceType.Patient, patientGuid);
        if (edsPatientId == null) {
            return null;
        }

        List<ResourceWrapper> resourceWrappers = resourceRepository.getResourcesByPatient(getServiceId(), edsPatientId);

        List<Resource> ret = new ArrayList<>();

        for (ResourceWrapper resourceWrapper: resourceWrappers) {
            String json = resourceWrapper.getResourceData();
            Resource resource = PARSER_POOL.parse(json);
            ret.add(resource);
        }

        return ret;
    }

    private void updateExistingObservationWithNewChildLinks(String locallyUniqueObservationId,
                                                            List<String> childResourceRelationships,
                                                            FhirResourceFiler fhirResourceFiler) throws Exception {

        Observation fhirObservation = (Observation)retrieveResource(locallyUniqueObservationId, ResourceType.Observation);
        if (fhirObservation == null) {
            //if the resource can't be found, it's because that EMIS observation record was saved as something other
            //than a FHIR Observation (example in the CSV test files is an Allergy that is linked to another Allergy)
            return;
        }

        //the EMIS patient GUID is part of the locallyUnique Id of the observation, to extract from that
        //String patientGuid = getPatientGuidFromUniqueId(locallyUniqueObservationId);

        boolean changed = false;

        for (String referenceValue : childResourceRelationships) {

            Reference reference = ReferenceHelper.createReference(referenceValue);
            Reference globallyUniqueReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(reference, fhirResourceFiler);

            //check if the parent observation doesn't already have our ob linked to it
            boolean alreadyLinked = false;
            for (Observation.ObservationRelatedComponent related: fhirObservation.getRelated()) {
                if (related.getType() == Observation.ObservationRelationshipType.HASMEMBER
                        && related.getTarget().equalsShallow(globallyUniqueReference)) {
                    alreadyLinked = true;
                    break;
                }
            }

            if (!alreadyLinked) {
                Observation.ObservationRelatedComponent fhirRelation = fhirObservation.addRelated();
                fhirRelation.setType(Observation.ObservationRelationshipType.HASMEMBER);
                fhirRelation.setTarget(globallyUniqueReference);

                changed = true;
            }
        }

        if (changed) {
            //make sure to pass in the parameter to bypass ID mapping, since this resource has already been done
            fhirResourceFiler.savePatientResource(null, false, new ObservationBuilder(fhirObservation));
        }
    }

    public void cacheProblemPreviousLinkedResources(String problemSourceId, List<Reference> previousReferences) {
        if (previousReferences == null
                || previousReferences.isEmpty()) {
            return;
        }

        //we know there will no CsvCells, so use this reference list class to save memory
        ReferenceList obj = new ReferenceListNoCsvCells();
        //ReferenceList obj = new ReferenceList();
        obj.add(previousReferences);

        problemPreviousLinkedResources.put(problemSourceId, obj);
    }

    public ReferenceList findProblemPreviousLinkedResources(String problemSourceId) {
        return problemPreviousLinkedResources.remove(problemSourceId);
    }

    public ReferenceList getAndRemoveNewProblemChildren(CsvCell problemGuid, CsvCell patientGuid) {
        String key = createUniqueId(patientGuid, problemGuid);
        return newProblemChildren.remove(key);
    }

    public void cacheProblemRelationship(String problemJournalId,
                                         CsvCell patientIdCell,
                                         CsvCell journalIdCell,
                                         ResourceType resourceType,
                                         CsvCell linksCell) {

        String key = createUniqueId(patientIdCell.getString(), problemJournalId);

        ReferenceList referenceList = newProblemChildren.get(key);
        if (referenceList == null) {
            referenceList = new ReferenceListSingleCsvCells(); //we know there will only one CsvCells, so use this reference list class to save memory
            newProblemChildren.put(key, referenceList);
        }

        String resourceLocalUniqueId = createUniqueId(patientIdCell, journalIdCell);
        Reference reference = ReferenceHelper.createReference(resourceType, resourceLocalUniqueId);
        referenceList.add(reference, linksCell);
    }

    /**
     * adds linked references to a FHIR problem, that may or may not already have linked references
     * returns true if any change was actually made, false otherwise
     */
    public static boolean addLinkedItemsToResource(DomainResource resource, List<Reference> references, String extensionUrl) {

        //see if we already have a list in the problem
        List_ list = null;

        if (resource.hasContained()) {
            for (Resource contained: resource.getContained()) {
                if (contained.getId().equals(CONTAINED_LIST_ID)) {
                    list = (List_)contained;
                }
            }
        }

        //if the list wasn't there before, create and add it
        if (list == null) {
            list = new List_();
            list.setId(CONTAINED_LIST_ID);
            resource.getContained().add(list);
        }

        //add the extension, unless it's already there
        boolean addExtension = !ExtensionConverter.hasExtension(resource, extensionUrl);
        if (addExtension) {
            Reference listReference = ReferenceHelper.createInternalReference(CONTAINED_LIST_ID);
            resource.addExtension(ExtensionConverter.createExtension(extensionUrl, listReference));
        }

        boolean changed = false;

        for (Reference reference : references) {

            //check to see if this resource is already linked to the problem
            boolean alreadyLinked = false;
            for (List_.ListEntryComponent entry: list.getEntry()) {
                Reference entryReference = entry.getItem();
                if (entryReference.getReference().equals(reference.getReference())) {
                    alreadyLinked = true;
                    break;
                }
            }

            if (!alreadyLinked) {
                list.addEntry().setItem(reference);
                changed = true;
            }
        }

        return changed;
    }

    private void addRelationshipsToExistingResource(String locallyUniqueResourceId,
                                                   ResourceType resourceType,
                                                   List<String> childResourceRelationships,
                                                   FhirResourceFiler fhirResourceFiler,
                                                   String extensionUrl) throws Exception {

        DomainResource fhirResource = (DomainResource)retrieveResource(locallyUniqueResourceId, resourceType);
        if (fhirResource == null) {
            //it's possible to create medication items that are linked to non-existent problems in Emis Web,
            //so ignore any data
            return;
        }

        //our resource is already ID mapped, so we need to manually map all the references in our list
        List<Reference> references = new ArrayList<>();

        for (String referenceValue : childResourceRelationships) {
            Reference reference = ReferenceHelper.createReference(referenceValue);
            Reference globallyUniqueReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(reference, fhirResourceFiler);
            references.add(globallyUniqueReference);
        }

        if (addLinkedItemsToResource(fhirResource, references, extensionUrl)) {

            //String patientGuid = getPatientGuidFromUniqueId(locallyUniqueResourceId);

            //make sure to pass in the parameter to bypass ID mapping, since this resource has already been done
            fhirResourceFiler.savePatientResource(null, false, new GenericBuilder(fhirResource));
        }
    }

    public static List<Reference> findPreviousLinkedReferences(VisionCsvHelper csvHelper,
                                                               FhirResourceFiler fhirResourceFiler,
                                                               String locallyUniqueId,
                                                               ResourceType resourceType) throws Exception {

        DomainResource previousVersion = (DomainResource)csvHelper.retrieveResource(locallyUniqueId, resourceType);
        if (previousVersion == null) {
            //if this is the first time, then we'll have a null resource
            return null;
        }
        List<Reference> ret = new ArrayList<>();

        if (previousVersion.hasContained()) {
            for (Resource contained: previousVersion.getContained()) {
                if (contained instanceof List_) {
                    List_ list = (List_)contained;
                    for (List_.ListEntryComponent entry: list.getEntry()) {
                        Reference previousReference = entry.getItem();

                        //the reference we have has already been mapped to an EDS ID, so we need to un-map it
                        //back to the source ID, so the ID mapper can safely map it when we save the resource
                        Reference unmappedReference = IdHelper.convertEdsReferenceToLocallyUniqueReference(csvHelper, previousReference);
                        if (unmappedReference != null) {
                            ret.add(unmappedReference);
                        }
                    }
                }
            }
        }

        return ret;
    }

    /*public void cacheDrugRecordDate(String drugRecordGuid, CsvCell patientGuid, DateTimeType dateTime) {
        String uniqueId = createUniqueId(patientGuid.getString(), drugRecordGuid);

        Date date = dateTime.getValue();

        DateType previous = drugRecordFirstIssueDateMap.get(uniqueId);
        if (previous == null
                || date.before(previous.getValue())) {
            drugRecordFirstIssueDateMap.put(uniqueId, new DateType(date));
        }

        previous = drugRecordLastIssueDateMap.get(uniqueId);
        if (previous == null
                || date.after(previous.getValue())) {
            drugRecordLastIssueDateMap.put(uniqueId, new DateType(date));
        }
    }

    public DateType getDrugRecordFirstIssueDate(CsvCell drugRecordId, CsvCell patientGuid) {
        return drugRecordFirstIssueDateMap.remove(createUniqueId(patientGuid, drugRecordId));
    }

    public DateType getDrugRecordLastIssueDate(CsvCell drugRecordId, CsvCell patientGuid) {
        return drugRecordLastIssueDateMap.remove(createUniqueId(patientGuid, drugRecordId));
    }*/

    public void cacheEthnicity(CsvCell patientIdCell, Date date, EthnicCategory ethnicCategory, CsvCell sourceCell) {
        String key = patientIdCell.getString();
        DateAndEthnicityCategory dc = ethnicityMap.get(new StringMemorySaver(key));
        if (dc == null
            || dc.isBefore(date)) {
            DateAndEthnicityCategory val = new DateAndEthnicityCategory(date, ethnicCategory, sourceCell);
            ethnicityMap.put(new StringMemorySaver(key), val);
        }
    }

    public DateAndEthnicityCategory findEthnicity(CsvCell patientIdCell) {
        String key = patientIdCell.getString();
        return ethnicityMap.remove(new StringMemorySaver(key));
    }


    public void cacheConsultationPreviousLinkedResources(String encounterSourceId, List<Reference> previousReferences) {

        if (previousReferences == null
                || previousReferences.isEmpty()) {
            return;
        }

        //we know there will be no CsvCells, so use this reference list class to save memory
        ReferenceList obj = new ReferenceListNoCsvCells();
        //ReferenceList obj = new ReferenceList();
        obj.add(previousReferences);

        consultationExistingChildMap.put(encounterSourceId, obj);
    }

    public ReferenceList findConsultationPreviousLinkedResources(String encounterSourceId) {
        return consultationExistingChildMap.remove(encounterSourceId);
    }

    public void cacheNewConsultationChildRelationship(String encounterId,
                                                      CsvCell patientIdCell,
                                                      CsvCell journalIdCell,
                                                      ResourceType resourceType,
                                                      CsvCell linksCell) {

        String consultationLocalUniqueId = createUniqueId(patientIdCell.getString(), encounterId);

        ReferenceList list = consultationNewChildMap.get(consultationLocalUniqueId);
        if (list == null) {
            list = new ReferenceListSingleCsvCells(); //we know there will a single CsvCell, so use this reference list class to save memory
            consultationNewChildMap.put(consultationLocalUniqueId, list);
        }

        String resourceLocalUniqueId = createUniqueId(patientIdCell, journalIdCell);
        Reference resourceReference = ReferenceHelper.createReference(resourceType, resourceLocalUniqueId);
        list.add(resourceReference, linksCell);
    }

    public ReferenceList getAndRemoveNewConsultationRelationships(String encounterSourceId) {
        return consultationNewChildMap.remove(encounterSourceId);
    }

    public Reference createEpisodeReference(CsvCell patientGuid) throws Exception{
        //we now use registration start date as part of the source ID for episodes of care, so we use the internal  ID map table to store that start date
        CsvCell regStartCell = getLatestEpisodeStartDate(patientGuid);
        if (regStartCell == null) {
            throw new Exception("Failed to find latest registration date for patient " + patientGuid);
        }
        String episodeSourceId = createUniqueId(patientGuid, regStartCell);
        return ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeSourceId);
    }

    public void cacheLatestEpisodeStartDate(CsvCell patientGuid, CsvCell startDateCell) throws Exception {
        String key = patientGuid.getString();
        String value = startDateCell.getString();

        latestEpisodeStartDateCache.put(key, value);
    }

    public CsvCell getLatestEpisodeStartDate(CsvCell patientGuid) throws Exception {
        String key = patientGuid.getString();
        String value = latestEpisodeStartDateCache.get(key);
        if (value == null) {

            //convert patient GUID to UUID
            String sourcePatientId = createUniqueId(patientGuid, null);
            UUID patientUuid = IdHelper.getEdsResourceId(serviceId, ResourceType.Patient, sourcePatientId);
            if (patientUuid == null) {
                return null;
            }

            Date latestDate = null;

            List<ResourceWrapper> episodeWrappers = resourceRepository.getResourcesByPatient(serviceId, patientUuid, ResourceType.EpisodeOfCare.toString());
            for (ResourceWrapper wrapper: episodeWrappers) {
                EpisodeOfCare episode = (EpisodeOfCare) FhirSerializationHelper.deserializeResource(wrapper.getResourceData());
                if (episode.hasPeriod()) {
                    Date d = episode.getPeriod().getStart();
                    if (latestDate == null
                            || d.after(latestDate)) {
                        latestDate = d;
                    }
                }
            }

            if (latestDate == null) {
                return null;
            }

            //need to convert back to a string in the same format as the raw file
            SimpleDateFormat sdf = new SimpleDateFormat(VisionCsvToFhirTransformer.DATE_FORMAT);
            value = sdf.format(latestDate);
            latestEpisodeStartDateCache.put(key, value);
        }

        return CsvCell.factoryDummyWrapper(value);
    }

    /*public void cacheProblemObservationGuid(CsvCell patientGuid, CsvCell problemGuid) {
        problemReadCodes.add(createUniqueId(patientGuid, problemGuid));
    }

    public boolean isProblemObservationGuid(String patientGuid, String problemGuid) {
        return problemReadCodes.contains(createUniqueId(patientGuid, problemGuid));
    }

    public void cacheDrugRecordGuid(CsvCell patientGuid, CsvCell drugRecordGuid) {
        drugRecords.add(createUniqueId(patientGuid, drugRecordGuid));
    }

    public boolean isDrugRecordGuid(String patientGuid, String drugRecordGuid) {
        return drugRecords.contains(createUniqueId(patientGuid, drugRecordGuid));
    }*/



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


    /**
     * caches the terms for Read2 codes. Some codes may have muptiple terms, and there's no clear way to work
     * out which is the "best" term, so our publishr_common table contains all of them
     */
    public void cacheCodeAndTermUsed(CsvCell readCodeCell, CsvCell termCell) throws Exception {
        if (readCodeCell.isEmpty()
                || termCell.isEmpty()) {
            return;
        }

        String readCode = VisionCodeHelper.formatReadCode(readCodeCell, this); //make sure to properly format the cell
        String term = termCell.getString();
        CodeAndTerm key = new CodeAndTerm(readCode, term);

        AtomicInteger count = hmRead2TermMap.get(key);
        if (count == null) {
            count = new AtomicInteger(0);
            hmRead2TermMap.put(key, count);
        }
        count.incrementAndGet();
    }

    /**
     * caches the Read2/Local code to Snomed concept mappings. There's subtle extra complexity here, because
     * we need to factor in the recorded date cell too. Vision has changed the Read2 to Snomed mappings over time
     * so we only want to keep the most recent ones.
     */
    public void cacheReadToSnomedMapping(CsvCell readCodeCell, CsvCell snomedCell, CsvCell recordedDateCell) throws Exception {

        String readCode = VisionCodeHelper.formatReadCode(readCodeCell, this); //make sure to properly format the cell
        Long snomedCode = VisionCodeHelper.formatSnomedConcept(snomedCell, this);

        if (readCode == null
                || snomedCode == null
                || recordedDateCell.isEmpty()) {
            return;
        }

        Date dRecorded = recordedDateCell.getDate();
        SnomedConceptAndDate existing = hmRead2toSnomedMap.get(readCode);

        //add to the map if not present or the existing one is older
        if (existing == null
                || existing.getDate().before(dRecorded)) {

            hmRead2toSnomedMap.put(readCode, new SnomedConceptAndDate(snomedCode, dRecorded));
        }
    }

    /**
     * saves the map of Read2 codes and their terms to the publisher_common DB so we have
     * some kind of reference table of Vision coding
     */
    public void saveCodeAndTermMaps(FhirResourceFiler fhirResourceFiler) throws Exception {

        //rather than hitting the DB record by record, for potentially thousands of codes,
        //we prepare a CSV file of the data and bulk insert it
        File tempDir = FileHelper.getTempDirRandomSubdirectory();
        File dstFile = new File(tempDir, "VisionRead2AndTerms.csv");
        LOG.trace("Writing Read2 term mappings to " + dstFile);

        FileOutputStream fos = new FileOutputStream(dstFile);
        OutputStreamWriter osw = new OutputStreamWriter(fos);
        BufferedWriter bufferedWriter = new BufferedWriter(osw);

        //for consistency with other, similar upload routines, use the Windows-style record separators
        CSVFormat format = EmisCsvToFhirTransformer.CSV_FORMAT
                .withHeader("Code", "Term", "UsageCount", "IsVisionCode")
                .withRecordSeparator("\r\n");

        CSVPrinter printer = new CSVPrinter(bufferedWriter, format);

        for (CodeAndTerm codeAndTerm: hmRead2TermMap.keySet()) {
            AtomicInteger count = hmRead2TermMap.get(codeAndTerm);
            String code = codeAndTerm.getCode();
            String term = codeAndTerm.getTerm();

            //the upload routine expects this boolean column to be a 1 or 0 (not "true" or "false")
            Integer isVisionCode;
            if (Read2Cache.isRealRead2Code(code)) {
                isVisionCode = new Integer(0);
            } else {
                isVisionCode = new Integer(1);
            }

            printer.printRecord(code, term, new Integer(count.get()), isVisionCode);
        }

        printer.close();
        LOG.trace("Written Read2 term mappings to " + dstFile);

        String filePath = dstFile.getAbsolutePath();
        Date dataDate = fhirResourceFiler.getDataDate();
        VisionCodeDalI dal = DalProvider.factoryVisionCodeDal();
        dal.updateRead2TermTable(filePath, dataDate);

        //tidy up after ourselves
        FileHelper.deleteRecursiveIfExists(tempDir);

        //set to null as we're finished with it now
        this.hmRead2TermMap = null;
    }

    /**
     * saves the map of Read2 codes to Snomed mappings to the pulisher_common DB
     */
    public void saveCodeToSnomedMaps(FhirResourceFiler fhirResourceFiler) throws Exception {
        //rather than hitting the DB record by record, for potentially thousands of codes,
        //we prepare a CSV file of the data and bulk insert it
        File tempDir = FileHelper.getTempDirRandomSubdirectory();
        File dstFile = new File(tempDir, "VisionRead2ToSnomedMap.csv");
        LOG.trace("Writing Read2 to Snomed mappings to " + dstFile);

        FileOutputStream fos = new FileOutputStream(dstFile);
        OutputStreamWriter osw = new OutputStreamWriter(fos);
        BufferedWriter bufferedWriter = new BufferedWriter(osw);

        //for consistency with other, similar upload routines, use the Windows-style record separators
        CSVFormat format = EmisCsvToFhirTransformer.CSV_FORMAT
                .withHeader("Read2", "SnomedConcept", "DateLastUsed")
                .withRecordSeparator("\r\n");

        CSVPrinter printer = new CSVPrinter(bufferedWriter, format);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd"); //format date to SQL format

        for (String code: hmRead2toSnomedMap.keySet()) {
            SnomedConceptAndDate value = hmRead2toSnomedMap.get(code);
            Long snomedConcept = value.getConcept();
            Date dRecorded = value.getDate();
            String dRecordedStr = simpleDateFormat.format(dRecorded);

            printer.printRecord(code, snomedConcept, dRecordedStr);
        }

        printer.close();
        LOG.trace("Written Read2 to Snomed mappings to " + dstFile);

        String filePath = dstFile.getAbsolutePath();
        Date dataDate = fhirResourceFiler.getDataDate();
        VisionCodeDalI dal = DalProvider.factoryVisionCodeDal();
        dal.updateRead2ToSnomedMapTable(filePath, dataDate);

        //tidy up after ourselves
        FileHelper.deleteRecursiveIfExists(tempDir);

        //set to null as we're finished with it now
        hmRead2toSnomedMap = null;
    }


    /**
     * when the transform is complete, if there's any values left in the ethnicity map
     * then we need to update pre-existing FHIR Patients with new data
     *
     * NOTE: Vision does not use Journal for Marital Status records (like Emis and TPP do) - see SD-187
     */
    public void processRemainingEthnicities(FhirResourceFiler fhirResourceFiler) throws Exception {

        //get a combined list of the keys (patientGuids) from both maps
        HashSet<StringMemorySaver> patientIds = new HashSet<>(ethnicityMap.keySet());

        for (StringMemorySaver key: patientIds) {

            DateAndEthnicityCategory val = ethnicityMap.get(key);
            String patientId = key.toString();

            Patient fhirPatient = (Patient)retrieveResource(patientId, ResourceType.Patient);
            if (fhirPatient == null) {
                //if we try to update the ethnicity on a deleted patient, or one we've never received, we'll get this exception, which is fine to ignore
                continue;
            }

            PatientBuilder patientBuilder = new PatientBuilder(fhirPatient);

            EthnicCategory ethnicCategory = val.getEthnicCategory();
            CsvCell sourceCell = val.getSourceCell();
            patientBuilder.setEthnicity(ethnicCategory, sourceCell);

            fhirResourceFiler.savePatientResource(null, false, patientBuilder);
        }
    }

    /**
     * if our journal file contained a link to one or more problems but the problems themselves
     * weren't transformed, then we need to update those problems with the new linked items
     */
    public void processRemainingProblemItems(FhirResourceFiler fhirResourceFiler) throws Exception {
        for (String localProblemId: newProblemChildren.keySet()) {
            ReferenceList newLinkedItems = newProblemChildren.get(localProblemId);

            Condition existingCondition = (Condition)retrieveResource(localProblemId, ResourceType.Condition);
            if (existingCondition == null) {
                //if the problem has been deleted, just skip it
                return;
            }

            ConditionBuilder conditionBuilder = new ConditionBuilder(existingCondition);
            ContainedListBuilder containedListBuilder = new ContainedListBuilder(conditionBuilder);

            boolean madeChange = false;

            for (int i=0; i<newLinkedItems.size(); i++) {
                Reference reference = newLinkedItems.getReference(i);
                CsvCell[] sourceCells = newLinkedItems.getSourceCells(i);

                //make sure to convert the reference into a DDS reference, since the Condition is already ID mapped
                Reference globallyUniqueReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(reference, fhirResourceFiler);

                boolean added = containedListBuilder.addReference(globallyUniqueReference, sourceCells);
                if (added) {
                    madeChange = true;
                }
            }

            if (madeChange) {
                fhirResourceFiler.savePatientResource(null, false, conditionBuilder);
            }
        }
    }

    public void processRemainingEncounterItems(FhirResourceFiler fhirResourceFiler) throws Exception {
        for (String localEncounterId: consultationNewChildMap.keySet()) {
            ReferenceList newLinkedItems = consultationNewChildMap.get(localEncounterId);

            Encounter existingEncounter = (Encounter)retrieveResource(localEncounterId, ResourceType.Encounter);
            if (existingEncounter == null) {
                //if deleted, just skip it
                return;
            }

            EncounterBuilder encounterBuilder = new EncounterBuilder(existingEncounter);
            ContainedListBuilder containedListBuilder = new ContainedListBuilder(encounterBuilder);

            boolean madeChange = false;

            for (int i=0; i<newLinkedItems.size(); i++) {
                Reference reference = newLinkedItems.getReference(i);
                CsvCell[] sourceCells = newLinkedItems.getSourceCells(i);

                //make sure to convert the reference into a DDS reference, since the Condition is already ID mapped
                Reference globallyUniqueReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(reference, fhirResourceFiler);

                boolean added = containedListBuilder.addReference(globallyUniqueReference, sourceCells);
                if (added) {
                    madeChange = true;
                }
            }

            if (madeChange) {
                fhirResourceFiler.savePatientResource(null, false, encounterBuilder);
            }
        }
    }

    private class SnomedConceptAndDate {
        private Long concept;
        private Date date;

        public SnomedConceptAndDate(Long concept, Date date) {
            this.concept = concept;
            this.date = date;
        }

        public Long getConcept() {
            return concept;
        }

        public Date getDate() {
            return date;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SnomedConceptAndDate that = (SnomedConceptAndDate) o;

            if (!concept.equals(that.concept)) return false;
            return date.equals(that.date);

        }

        @Override
        public int hashCode() {
            int result = concept.hashCode();
            result = 31 * result + date.hashCode();
            return result;
        }
    }

    private class CodeAndTerm {
        private String code;
        private String term;

        public CodeAndTerm(String code, String term) {
            this.code = code;
            this.term = term;
        }

        public String getCode() {
            return code;
        }

        public String getTerm() {
            return term;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CodeAndTerm that = (CodeAndTerm) o;

            if (!code.equals(that.code)) return false;
            return term.equals(that.term);

        }

        @Override
        public int hashCode() {
            int result = code.hashCode();
            result = 31 * result + term.hashCode();
            return result;
        }
    }


    /**
     * temporary storage class for a CodeableConcept and Date
     */
    public class DateAndEthnicityCategory {
        private Date date;
        private EthnicCategory ethnicCategory = null;
        private CsvCell sourceCell;

        public DateAndEthnicityCategory(Date date, EthnicCategory ethnicCategory, CsvCell sourceCell) {
            this.date = date;
            this.ethnicCategory = ethnicCategory;
            this.sourceCell = sourceCell;
        }

        public Date getDate() {
            return date;
        }

        public EthnicCategory getEthnicCategory() {
            return ethnicCategory;
        }

        public CsvCell getSourceCell() {
            return sourceCell;
        }

        public boolean isBefore(Date other) {
            if (date == null) {
                return true;
            } else if (other == null) {
                return false;
            } else {
                return date.before(other);
            }

        }
    }

    public static String cleanUserId(String data) {
        if (!Strings.isNullOrEmpty(data)) {
            if (data.contains(":STAFF:")) {
                data = data.replace(":","").replace("STAFF","");
                return data;
            }
            if (data.contains(":EXT_STAFF:")) {
                data = data.substring(0,data.indexOf(","));
                data = data.replace(":","").replace("EXT_STAFF","").replace(",", "");
                return data;
            }
        }
        return data;
    }


    public void submitToThreadPool(Callable callable) throws Exception {
        if (this.utilityThreadPool == null) {
            int threadPoolSize = ConnectionManager.getPublisherTransformConnectionPoolMaxSize(serviceId);
            this.utilityThreadPool = new ThreadPool(threadPoolSize, 1000, "VisionCsvHelper"); //lower from 50k to save memory
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

}
