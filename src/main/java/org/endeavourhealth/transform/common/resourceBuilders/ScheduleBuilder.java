package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;

public class ScheduleBuilder extends ResourceBuilderBase {

    private Schedule schedule = null;

    public ScheduleBuilder() {
        this(null);
    }

    public ScheduleBuilder(Schedule schedule) {
        this(schedule, null);
    }

    public ScheduleBuilder(Schedule schedule, ResourceFieldMappingAudit audit) {
        super(audit);

        this.schedule = schedule;
        if (this.schedule == null) {
            this.schedule = new Schedule();
            this.schedule.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_SCHEDULE));
        }
    }

    @Override
    public DomainResource getResource() {
        return schedule;
    }

    public void setLocation(Reference locationReference, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateExtension(this.schedule, FhirExtensionUri.SCHEDULE_LOCATION, locationReference);

        auditReferenceExtension(extension, sourceCells);
    }

    public void addComment(String comment, CsvCell... sourceCells) {

        //note this will append to any existing comment, with a newline char between
        String existingComment = this.schedule.getComment();
        if (!Strings.isNullOrEmpty(existingComment)) {
            existingComment += "\n";
        } else {
            existingComment = "";
        }
        existingComment += comment;

        this.schedule.setComment(existingComment);

        auditValue("comment", sourceCells);
    }

    public void setTypeFreeText(String type, CsvCell... sourceCells) {
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(type);
        this.schedule.addType(codeableConcept);

        int index = this.schedule.getType().size()-1;
        auditValue("type[" + index + "].text", sourceCells);
    }

    private Period getOrCreatePlanningHorizon() {
        Period period = this.schedule.getPlanningHorizon();
        if (period == null) {
            period = new Period();
            this.schedule.setPlanningHorizon(period);
        }
        return period;
    }

    public void setPlanningHorizonStart(Date date, CsvCell... sourceCells) {
        Period period = getOrCreatePlanningHorizon();
        period.setStart(date);

        auditValue("planningHorizon.start", sourceCells);
    }

    public void setPlanningHorizonEnd(Date date, CsvCell... sourceCells) {
        Period period = getOrCreatePlanningHorizon();
        period.setEnd(date);

        auditValue("planningHorizon.end", sourceCells);
    }

    public void clearActors() {
        this.schedule.setActor(null);

        for (int i=this.schedule.getExtension().size()-1; i>=0; i--) {
            Extension extension = schedule.getExtension().get(i);
            if (extension.getUrl().equals(FhirExtensionUri.SCHEDULE_ADDITIONAL_ACTOR)) {
                schedule.getExtension().remove(i);
            }
        }
    }

    public void addActor(Reference practitionerReference, CsvCell... sourceCells) {
        //the schedule resource only supports ONE actor, so put the first there and
        //all subsequent ones in extensions
        if (!this.schedule.hasActor()) {
            this.schedule.setActor(practitionerReference);

            auditValue("actor.reference", sourceCells);

        } else {
            //we can have multiple actors, so always just create a new extension
            Extension extension = ExtensionConverter.createExtension(FhirExtensionUri.SCHEDULE_ADDITIONAL_ACTOR, practitionerReference);
            this.schedule.getExtension().add(extension);

            auditReferenceExtension(extension, sourceCells);
        }
    }
}
