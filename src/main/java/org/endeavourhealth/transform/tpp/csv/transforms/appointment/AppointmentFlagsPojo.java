package org.endeavourhealth.transform.tpp.csv.transforms.appointment;

import org.endeavourhealth.transform.common.CsvCell;

public class AppointmentFlagsPojo {

   private CsvCell RowIdentifier;
            private CsvCell idOrganisationVisibleTo;
            private CsvCell idAppointment;
            private CsvCell flag;
            private CsvCell oldRowIdentifier;
            private CsvCell removedData;

    public AppointmentFlagsPojo() {
    }

    public CsvCell getRowIdentifier() {
        return RowIdentifier;
    }

    public void setRowIdentifier(CsvCell rowIdentifier) {
        RowIdentifier = rowIdentifier;
    }

    public CsvCell getIdOrganisationVisibleTo() {
        return idOrganisationVisibleTo;
    }

    public void setIdOrganisationVisibleTo(CsvCell idOrganisationVisibleTo) {
        this.idOrganisationVisibleTo = idOrganisationVisibleTo;
    }

    public CsvCell getIdAppointment() {
        return idAppointment;
    }

    public void setIdAppointment(CsvCell idAppointment) {
        this.idAppointment = idAppointment;
    }

    public CsvCell getFlag() {
        return flag;
    }

    public void setFlag(CsvCell flag) {
        this.flag = flag;
    }

    public CsvCell getOldRowIdentifier() {
        return oldRowIdentifier;
    }

    public void setOldRowIdentifier(CsvCell oldRowIdentifier) {
        this.oldRowIdentifier = oldRowIdentifier;
    }

    public CsvCell getRemovedData() {
        return removedData;
    }

    public void setRemovedData(CsvCell removedData) {
        this.removedData = removedData;
    }
}
