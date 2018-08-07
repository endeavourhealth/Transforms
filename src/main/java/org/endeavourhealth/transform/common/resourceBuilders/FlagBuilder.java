package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;
import java.util.List;

public class FlagBuilder extends ResourceBuilderBase
                            implements HasIdentifierI {


    private Flag flag = null;

    public FlagBuilder() {
        this(null);
    }

    public FlagBuilder(Flag flag) {
        this(flag, null);
    }

    public FlagBuilder(Flag flag, ResourceFieldMappingAudit audit) {
        super(audit);

        this.flag = flag;
        if (this.flag == null) {
            this.flag = new Flag();
            this.flag.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_FLAG));
        }
    }

    public void setStatus(Flag.FlagStatus status, CsvCell... sourceCells) {
        this.flag.setStatus(status);

        auditValue("status", sourceCells);
    }

    public void setCategory(String category, CsvCell... sourceCells) {
        if (!Strings.isNullOrEmpty(category)) {
            CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(category);
            this.flag.setCategory(codeableConcept);

            auditValue("category", sourceCells);
        }
    }

    public void setCode(String code, CsvCell... sourceCells) {
        if (!Strings.isNullOrEmpty(code)) {
            CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(code);
            this.flag.setCode(codeableConcept);

            auditValue("code", sourceCells);
        }
    }

    public void setSubject(Reference patientReference, CsvCell... sourceCells) {
        this.flag.setSubject(patientReference);

        auditValue("subject", sourceCells);
    }

    private Period getOrCreateNamePeriod() {
        Period period = null;
        if (flag.hasPeriod()) {
            period = flag.getPeriod();
        } else {
            period = new Period();
            flag.setPeriod(period);
        }
        return period;
    }

    public void setStartDate(Date date, CsvCell... sourceCells) {
        getOrCreateNamePeriod().setStart(date);

        auditValue("period.start", sourceCells);
    }

    public void setEndDate(Date date, CsvCell... sourceCells) {
        getOrCreateNamePeriod().setEnd(date);

        auditValue("period.end", sourceCells);
    }

    public void setEncounter(Reference encounterReference, CsvCell... sourceCells) {
        this.flag.setEncounter(encounterReference);
        auditValue("encounter", sourceCells);
    }

    public void setAuthor(Reference personReference, CsvCell... sourceCells) {
        this.flag.setAuthor(personReference);

        auditValue("author", sourceCells);
    }

    @Override
    public DomainResource getResource() {
        return flag;
    }

    @Override
    public Identifier addIdentifier() {
        return this.flag.addIdentifier();
    }

    @Override
    public String getIdentifierJsonPrefix(Identifier identifier) {
        int index = this.flag.getIdentifier().indexOf(identifier);
        return "identifier[" + index + "]";
    }

    @Override
    public List<Identifier> getIdentifiers() {
        return this.flag.getIdentifier();
    }

    @Override
    public void removeIdentifier(Identifier identifier) {
        this.flag.getIdentifier().remove(identifier);
    }
}
