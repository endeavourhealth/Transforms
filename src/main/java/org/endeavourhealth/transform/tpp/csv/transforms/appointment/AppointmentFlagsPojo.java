package org.endeavourhealth.transform.tpp.csv.transforms.appointment;

import org.endeavourhealth.transform.common.CsvCell;

public class AppointmentFlagsPojo {

   private CsvCell RowIdentifier;
            private CsvCell IDOrganisationVisibleTo;
            private CsvCell IDAppointment;
            private CsvCell Flag;
            private CsvCell OldRowIdentifier;
            private CsvCell RemovedData;

    public AppointmentFlagsPojo() {
    }

    public CsvCell getRowIdentifier() {
        return RowIdentifier;
    }

    public void setRowIdentifier(CsvCell rowIdentifier) {
        RowIdentifier = rowIdentifier;
    }

    public CsvCell getIDOrganisationVisibleTo() {
        return IDOrganisationVisibleTo;
    }

    public void setIDOrganisationVisibleTo(CsvCell IDOrganisationVisibleTo) {
        this.IDOrganisationVisibleTo = IDOrganisationVisibleTo;
    }

    public CsvCell getIDAppointment() {
        return IDAppointment;
    }

    public void setIDAppointment(CsvCell IDAppointment) {
        this.IDAppointment = IDAppointment;
    }

    public CsvCell getFlag() {
        return Flag;
    }

    public void setFlag(CsvCell flag) {
        Flag = flag;
    }

    public CsvCell getOldRowIdentifier() {
        return OldRowIdentifier;
    }

    public void setOldRowIdentifier(CsvCell oldRowIdentifier) {
        OldRowIdentifier = oldRowIdentifier;
    }

    public CsvCell getRemovedData() {
        return RemovedData;
    }

    public void setRemovedData(CsvCell removedData) {
        RemovedData = removedData;
    }
}
