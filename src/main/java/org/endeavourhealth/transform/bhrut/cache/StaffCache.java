package org.endeavourhealth.transform.bhrut.cache;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.TppStaffDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppStaffMember;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppStaffMemberProfile;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.TransformConfig;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class StaffCache {
    // Basic Consultant Code to consultant name cache
    private static final Logger LOG = LoggerFactory.getLogger(StaffCache.class);

    private Map<String, String> consultantCodeToName = new ConcurrentHashMap<>();

    public void addConsultantCode(CsvCell consultantCodeCell, CsvCell consultantName) {

        String cCode = consultantCodeCell.getString();
        String name = consultantName.getString();
        if (!consultantCodeToName.containsKey(cCode)) {
            consultantCodeToName.put(cCode, name);
        }
    }

    public boolean cCodeInCache(String cCode) {
        return consultantCodeToName.containsKey(cCode);
    }

    public String getNameForCcode(String cCode) {
        if (!cCodeInCache(cCode)) {
            return null;
        }
        return consultantCodeToName.get(cCode);
    }

    public void clear() {
        consultantCodeToName.clear();
    }

    public long size() {
        return consultantCodeToName.size();
    }
}
