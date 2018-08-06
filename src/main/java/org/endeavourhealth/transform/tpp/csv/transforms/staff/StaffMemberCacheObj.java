package org.endeavourhealth.transform.tpp.csv.transforms.staff;

import org.endeavourhealth.transform.common.CsvCell;

public class StaffMemberCacheObj {

    private CsvCell fullName;
    private CsvCell userName;
    private CsvCell nationalId;
    private CsvCell nationalIdType;
    private CsvCell smartCardId;
    private CsvCell obsolete;

    public StaffMemberCacheObj(CsvCell fullName, CsvCell userName, CsvCell nationalId, CsvCell nationalIdType, CsvCell smartCardId, CsvCell obsolete) {
        this.fullName = fullName;
        this.userName = userName;
        this.nationalId = nationalId;
        this.nationalIdType = nationalIdType;
        this.smartCardId = smartCardId;
        this.obsolete = obsolete;
    }

    public CsvCell getFullName() {
        return fullName;
    }

    public CsvCell getUserName() {
        return userName;
    }

    public CsvCell getNationalId() {
        return nationalId;
    }

    public CsvCell getNationalIdType() {
        return nationalIdType;
    }

    public CsvCell getSmartCardId() {
        return smartCardId;
    }

    public CsvCell getObsolete() {
        return obsolete;
    }
}
