package org.endeavourhealth.transform.barts.cache;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.endeavourhealth.transform.common.CsvCell;

// basic pojo for cached encounter fields needed by ProceTransformer
public class ProcedurePojo {

    /*
     *
     * */

    private String exchangeId;
    private CsvCell encounterId;
    private CsvCell DOB;
    private CsvCell MRN;
    private CsvCell ward;
    private CsvCell site;
    private CsvCell consultant;
    private CsvCell proc_dt_tm;
    private CsvCell create_dt_tm;
    private CsvCell updatedBy;
    private CsvCell notes;
    private CsvCell Mrn;
    private CsvCell procedureCode;
    private CsvCell procedureCodeType;
    private int comparisonCode;

    public String getExchangeId() {
        return exchangeId;
    }

    public void setExchangeId(String exchangeId) {
        this.exchangeId = exchangeId;
    }

    public void setComparisonCode(int comparisonCode) {
        this.comparisonCode = comparisonCode;
    }

    public CsvCell getProcedureCodeType() {
        return procedureCodeType;
    }

    public void setProcedureCodeType(CsvCell procedureCodeType) {
        this.procedureCodeType = procedureCodeType;
    }

    public CsvCell getCreate_dt_tm() {
        return create_dt_tm;
    }

    public void setCreate_dt_tm(CsvCell create_dt_tm) {
        this.create_dt_tm = create_dt_tm;
    }

    public CsvCell getWard() {
        return ward;
    }

    public void setWard(CsvCell ward) {
        this.ward = ward;
    }

    public CsvCell getSite() {
        return site;
    }

    public void setSite(CsvCell site) {
        this.site = site;
    }

    public CsvCell getDOB() {
        return DOB;
    }

    public void setDOB(CsvCell DOB) {
        this.DOB = DOB;
    }

    public CsvCell getMRN() {
        return MRN;
    }

    public void setMRN(CsvCell MRN) {
        this.MRN = MRN;
    }

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

    public void setComparisonCode() {
        this.comparisonCode = hashCode();
    }

    public int getComparisonCode() {
        return this.comparisonCode;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(encounterId)
                .append(DOB)
                .append(MRN)
                .append(ward)
                .append(site)
                .append(consultant)
                .append(proc_dt_tm)
                .append(create_dt_tm)
                .append(updatedBy)
                .append(notes)
                .append(Mrn)
                .append(procedureCode)
                .append(procedureCodeType)
                .toHashCode();
    }

//    public static BartsStagingDataProcedure toBartsStagingData(ProcedurePojo in) throws  Exception{
//
//        BartsStagingDataProcedure ret = new BartsStagingDataProcedure();
//        ret.setExchangeId(in.getExchangeId());
//        ret.setEncounterId(in.getEncounterId().getInt());
//        ret.setPersonId(in.getMRN().getInt());
//        ret.setWard(in.getWard().getString());
//        ret.setSite(in.getSite().getString());
//        ret.setConsultant(in.getConsultant().getString());
//        ret.setProc_dt_tm(in.getProc_dt_tm().getDate());
//        ret.setCreate_dt_tm(in.getCreate_dt_tm().getDate());
//        ret.setUpdatedBy(in.getUpdatedBy().getInt());
//        ret.setNotes(in.getNotes().getString());
//        ret.setProcedureCode(in.getProcedureCode().getString());
//        ret.setProcedureCodeType(in.getProcedureCodeType().getString());
//        ret.setComparisonCode(in.getComparisonCode());
//
//        return ret;
//
//    }
}
