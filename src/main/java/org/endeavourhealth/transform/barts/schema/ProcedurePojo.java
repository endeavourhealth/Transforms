package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.common.CsvCell;

// basic pojo for cached encounter fields needed by ProceTransformer
public class ProcedurePojo {

    private CsvCell encounterId;
    private CsvCell consultant;
    private CsvCell proc_dt_tm;
    private CsvCell updatedBy;
    private CsvCell notes;
    private CsvCell Mrn;
    private CsvCell procedureCode;

    public CsvCell getMrn() {
        return Mrn;
    }

    public void setMrn(CsvCell mrn) {
        Mrn = mrn;
    }

    public CsvCell getProcedureCode() {return procedureCode;}

    public void setProcedureCode(CsvCell procedureCode) {this.procedureCode = procedureCode;}

    public CsvCell getNotes() {
        return notes;
    }

    public void setNotes(CsvCell notes) {
        this.notes = notes;
    }

    public CsvCell getEncounterId() {
        return encounterId;
    }

    public void setEncounterId(CsvCell encounterId) {
        this.encounterId = encounterId;
    }

    public CsvCell getConsultant() {
        return consultant;
    }

    public void setConsultant(CsvCell consultant) {
        this.consultant = consultant;
    }

    public CsvCell getProc_dt_tm() {
        return proc_dt_tm;
    }

    public void setProc_dt_tm(CsvCell proc_dt_tm) {
        this.proc_dt_tm = proc_dt_tm;
    }

    public CsvCell getUpdatedBy() {
        return updatedBy;
    }

    public void setUpdatedBy(CsvCell updatedBy) {
        this.updatedBy = updatedBy;
    }
}
