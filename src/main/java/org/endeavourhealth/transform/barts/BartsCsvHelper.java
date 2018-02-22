package org.endeavourhealth.transform.barts;

import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherTransform.CernerCodeValueRefDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.IdHelper;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.HashMap;
import java.util.UUID;

public class BartsCsvHelper {

    public static final String CODE_TYPE_SNOMED = "SNOMED";
    public static final String CODE_TYPE_ICD_10 = "ICD10WHO";

    private static CernerCodeValueRefDalI cernerCodeValueRefDalI = DalProvider.factoryCernerCodeValueRefDal();
    private static HashMap<String, CernerCodeValueRef> cernerCodes = new HashMap<>();

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

    public Resource retrieveResource(String locallyUniqueId, ResourceType resourceType) throws Exception {

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
        return ParserPool.getInstance().parse(json);
    }

    public Reference createPractitionerReference(String practitionerGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Practitioner, practitionerGuid);
    }

    public Reference createPractitionerReference(CsvCell practitionerIdCell) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Practitioner, practitionerIdCell.getString());
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

        // Add to the cache
        cernerCodes.put(codeLookup, cernerCodeFromDB);

        return cernerCodeFromDB;
    }
}
