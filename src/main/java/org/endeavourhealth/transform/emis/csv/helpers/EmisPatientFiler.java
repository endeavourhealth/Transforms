package org.endeavourhealth.transform.emis.csv.helpers;

import org.endeavourhealth.core.database.dal.audit.models.Exchange;
import org.endeavourhealth.core.database.dal.audit.models.HeaderKeys;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class EmisPatientFiler {
    private static final Logger LOG = LoggerFactory.getLogger(EmisPatientFiler.class);

    private Set<String> filterPatientGuids;

    private EmisPatientFiler(Set<String> filterPatientGuids) {
        this.filterPatientGuids = filterPatientGuids;
    }

    public static EmisPatientFiler factory(Exchange exchange) throws Exception {

        List<String> patientGuids = exchange.getHeaderAsStringList(HeaderKeys.EmisPatientGuids);
        if (patientGuids == null) {
            return new EmisPatientFiler(null);
        } else {
            LOG.info("Filtering on " + patientGuids.size() + " Emis patient GUIDs");
            return new EmisPatientFiler(new HashSet<>(patientGuids));
        }
    }

    public boolean shouldProcessRecord(AbstractCsvParser parser) {
        CsvCell patientGuidCell = parser.getCell("PatientGuid");

        //if the record doesn't have a PatientGUID (e.g. organisation file) then always process
        if (patientGuidCell == null) {
            return true;
        }

        return shouldProcessRecord(patientGuidCell);
    }

    public boolean shouldProcessRecord(CsvCell patientGuidCell) {
        String patientGuid = patientGuidCell.getString();
        return shouldProcessRecord(patientGuid);
    }

    public boolean shouldProcessRecord(String patientGuid) {

        //if no filtering, always return true
        if (this.filterPatientGuids == null
                || this.filterPatientGuids.isEmpty()) {
            return true;
        }

        //check our filtered set of GUIDs
        if (this.filterPatientGuids.contains(patientGuid)) {
            return true;
        }

        return false;
    }

}
