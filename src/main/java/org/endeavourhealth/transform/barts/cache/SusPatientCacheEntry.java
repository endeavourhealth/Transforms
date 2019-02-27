package org.endeavourhealth.transform.barts.cache;

import org.endeavourhealth.transform.common.CsvCell;

import java.util.ArrayList;
import java.util.List;

/**
 * object used to cache SUS Patient records in memory. Although the files are slightly different between
 * Inpatient, Outpatient and Emergency, they're the same as far as the fields we're interested in go
 */
public class SusPatientCacheEntry {
    private CsvCell CDSUniqueIdentifier;
    private CsvCell localPatientId;
    private CsvCell NHSNumber;
    private CsvCell cdsActivityDate;
    private CsvCell PrimaryProcedureOPCS;
    private CsvCell PrimaryProcedureDate;
    private CsvCell SecondaryProcedureOPCS;
    private CsvCell SecondaryProcedureDate;
    private CsvCell OtherSecondaryProceduresOPCS;
    private List<String> otherCodes= new ArrayList();

    public List<String> getOtherCodes() {
        return otherCodes;
    }

    public void setOtherCodes(List<String> otherCodes) {
        this.otherCodes = otherCodes;
    }

    public CsvCell getCdsActivityDate() {
        return cdsActivityDate;
    }

    public void setCdsActivityDate(CsvCell cdsActivityDate) {
        this.cdsActivityDate = cdsActivityDate;
    }

    public CsvCell getCDSUniqueIdentifier() {
        return CDSUniqueIdentifier;
    }

    public void setCDSUniqueIdentifier(CsvCell CDSUniqueIdentifier) {
        this.CDSUniqueIdentifier = CDSUniqueIdentifier;
    }

    public CsvCell getLocalPatientId() {
        return localPatientId;
    }

    public void setLocalPatientId(CsvCell localPatientId) {
        this.localPatientId = localPatientId;
    }

    public CsvCell getNHSNumber() {
        return NHSNumber;
    }

    public void setNHSNumber(CsvCell NHSNumber) {
        this.NHSNumber = NHSNumber;
    }

    public CsvCell getPrimaryProcedureOPCS() {
        return PrimaryProcedureOPCS;
    }

    public void setPrimaryProcedureOPCS(CsvCell primaryProcedureOPCS) {
        PrimaryProcedureOPCS = primaryProcedureOPCS;
    }

    public CsvCell getPrimaryProcedureDate() {
        return PrimaryProcedureDate;
    }

    public void setPrimaryProcedureDate(CsvCell primaryProcedureDate) {
        PrimaryProcedureDate = primaryProcedureDate;
    }

    public CsvCell getSecondaryProcedureOPCS() {
        return SecondaryProcedureOPCS;
    }

    public void setSecondaryProcedureOPCS(CsvCell secondaryProcedureOPCS) {
        SecondaryProcedureOPCS = secondaryProcedureOPCS;
    }

    public CsvCell getSecondaryProcedureDate() {
        return SecondaryProcedureDate;
    }

    public void setSecondaryProcedureDate(CsvCell secondaryProcedureDate) {
        SecondaryProcedureDate = secondaryProcedureDate;
    }

    public CsvCell getOtherSecondaryProceduresOPCS() {
        return OtherSecondaryProceduresOPCS;
    }

    public void setOtherSecondaryProceduresOPCS(CsvCell otherSecondaryProceduresOPCS) {
        OtherSecondaryProceduresOPCS = otherSecondaryProceduresOPCS;
    }
}
