package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.DomainResource;
import org.hl7.fhir.instance.model.Meta;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.Slot;

import java.util.Date;

public class SlotBuilder extends ResourceBuilderBase {

    private Slot slot = null;

    public SlotBuilder() {
        this(null);
    }

    public SlotBuilder(Slot slot) {
        this.slot = slot;
        if (this.slot == null) {
            this.slot = new Slot();
            this.slot.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_SLOT));
        }
    }

    @Override
    public DomainResource getResource() {
        return slot;
    }

    public void setSchedule(Reference scheduleReference, CsvCell... sourceCells) {
        this.slot.setSchedule(scheduleReference);

        auditValue("schedule.reference", sourceCells);
    }

    public void setFreeBusyType(Slot.SlotStatus status, CsvCell... sourceCells) {
        this.slot.setFreeBusyType(status);

        auditValue("freeBusyType", sourceCells);
    }

    public void setStartDateTime(Date startDateTime, CsvCell... sourceCells) {
        this.slot.setStart(startDateTime);

        auditValue("start", sourceCells);
    }

    public void setEndDateTime(Date endDateTime, CsvCell... sourceCells) {
        this.slot.setEnd(endDateTime);

        auditValue("end", sourceCells);
    }
}
