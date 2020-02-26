package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.common.CsvCell;

public interface CdsRecordInpatientI extends CdsRecordI {

    CsvCell getHospitalSpellNumber();

    CsvCell getAdmissionMethodCode();

    CsvCell getAdmissionSourceCode();

    CsvCell getPatientClassification();

    CsvCell getHospitalSpellStartDate();

    CsvCell getHospitalSpellStartTime();

    CsvCell getEpisodeNumber();

    CsvCell getEpisodeStartSiteCode();

    CsvCell getEpisodeStartWardCode();

    CsvCell getEpisodeStartDate();

    CsvCell getEpisodeStartTime();

    CsvCell getEpisodeEndSiteCode();

    CsvCell getEpisodeEndWardCode();

    CsvCell getEpisodeEndDate();

    CsvCell getEpisodeEndTime();

    CsvCell getDischargeDate();

    CsvCell getDischargeTime();

    CsvCell getDischargeDestinationCode();

    CsvCell getDischargeMethod();

    CsvCell getTotalPreviousPregnancies();

    CsvCell getNumberOfBabies();

    CsvCell getFirstAntenatalAssessmentDate();

    CsvCell getAntenatalCarePractitioner();

    CsvCell getAntenatalCarePractice();

    CsvCell getDeliveryPlaceTypeIntended();

    CsvCell getDeliveryPlaceChangeReasonCode();

    CsvCell getGestationLengthLabourOnset();

    CsvCell getDeliveryDate();

    CsvCell getMotherNHSNumber();

    CsvCell getDeliveryPlaceTypeActual();

    CsvCell getBirthOrder(int dataNumber);
    CsvCell getDeliveryMethod(int dataNumber);
    CsvCell getBabyNHSNumber(int dataNumber);
    CsvCell getBabyBirthDate(int dataNumber);
    CsvCell getBirthWeight(int dataNumber);
    CsvCell getLiveOrStillBirthIndicator(int dataNumber);
    CsvCell getBabyGender(int dataNumber);
}