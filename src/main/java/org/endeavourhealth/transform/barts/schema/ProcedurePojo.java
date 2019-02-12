package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.common.CsvCell;

// basic pojo for cached encounter fields needed by ProceTransformer
public class ProcedurePojo {

    private CsvCell encounterId;
    private CsvCell consultant;
    private CsvCell create_dt_tm;
    private CsvCell updatedBy;
    private CsvCell notes;
    private CsvCell Mrn;
    private CsvCell procedureCode;
    private String procedureCodeValueText;
    private CsvCell procedureCodeType;

    public String getProcedureCodeValueText() {
        return procedureCodeValueText;
    }

    public void setProcedureCodeValueText(String procedureCodeValueText) {
        this.procedureCodeValueText = procedureCodeValueText;
    }

    public CsvCell getProcedureCodeType() {return procedureCodeType;}

    public void setProcedureCodeType(CsvCell procedureCodeType) {this.procedureCodeType = procedureCodeType;}

    public CsvCell getMrn() {
        return Mrn;
    }

    public void setMrn(CsvCell mrn) {
        Mrn = mrn;
    }

    public CsvCell getProcedureCode() {
        return procedureCode;
    }

    public void setProcedureCode(CsvCell procedureCode) {
        this.procedureCode = procedureCode;
    }

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

    public CsvCell getCreate_dt_tm() {
        return create_dt_tm;
    }

    public void setCreate_dt_tm(CsvCell create_dt_tm) {
        this.create_dt_tm = create_dt_tm;
    }

    public CsvCell getUpdatedBy() {
        return updatedBy;
    }

    public void setUpdatedBy(CsvCell updatedBy) {
        this.updatedBy = updatedBy;
    }
}
