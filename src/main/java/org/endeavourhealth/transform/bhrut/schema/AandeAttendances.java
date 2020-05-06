package org.endeavourhealth.transform.bhrut.schema;

import org.endeavourhealth.transform.bhrut.BhrutCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class AandeAttendances extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(AandeAttendances.class); 

  public AandeAttendances(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
            super(serviceId, systemId, exchangeId, version, filePath,
                    BhrutCsvToFhirTransformer.CSV_FORMAT,
                    BhrutCsvToFhirTransformer.DATE_FORMAT,
                    BhrutCsvToFhirTransformer.TIME_FORMAT);
        }


        @Override
        protected String[] getCsvHeaders(String version) {
            return new String[]{
                      "LineStatus",
                       "EXTERNAL_ID",
                       "ATTENDANCE_NUMBER",
                       "PAS_ID",
                       "HOSPITAL_NAME",
                       "HOSPITAL_CODE",
                       "HOSPITAL_SITE",
                       "ATTENDANCE_TYPE",
                       "FIRST_LOCATION",
                       "LAST_LOCATION",
                       "ARRIVAL_MODE",
                       "REFERRAL_SOURCE",
                       "ARRIVAL_DTTM",
                       "REGISTRATION_DTTM",
                       "RAT_DTTM",
                       "TRIAGE_DTTM",
                       "REFERRED_AE_DOCTOR_DTTM",
                       "SEEN_BY_AE_DOCTOR_DTTM",
                       "REFERRED_TO_PATHOLOGY_DTTM",
                       "BACK_FROM_PATHOLOGY_DTTM",
                       "REFERRED_TO_XRAY_DTTM",
                       "BACK_FROM_XRAY_DTTM",
                       "REFERRED_TO_TREATMENT_DTTM",
                       "BACK_FROM_TREATMENT_DTTM",
                       "FIRST_REFERRED_TO_SPECIALTY_DTTM",
                       "FIRST_SEEN_BY_SPECIALTY_DTTM",
                       "FIRST_SPECIALTY_REFERRED",
                       "LAST_REFERRED_TO_SPECIALTY_DTTM",
                       "LAST_SEEN_BY_SPECIALTY_DTTM",
                       "LAST_SPECIALTY_REFERRED",
                       "BED_REQUEST_DTTM",
                       "BED_REQUEST_OUTCOME_DTTM",
                       "MINUTES_TO_BED_REQUEST",
                       "CAU_BED_REQUEST_DTTM",
                       "MAU_BED_REQUEST_DTTM",
                       "SAU_BED_REQUEST_DTTM",
                       "TRANSPORT_REQUEST_DTTM",
                       "TRANSPORT_OUTCOME_DTTM",
                       "DIAGNOSIS_RECORD_DTTM",
                       "CLINICIANS_NAME",
                       "COMPLAINT",
                       "DISCHARGED_DTTM",
                       "LEFT_DEPARTMENT_DTTM",
                       "DISCHARGE_DESTINATION",
                       "RECORDED_OUTCOME"
                    

            };

        }
 public CsvCell getLinestatus() { return super.getCell("LineStatus");}
 public CsvCell getId() { return super.getCell( "EXTERNAL_ID");}
 public CsvCell getAttendanceNumber() { return super.getCell( "ATTENDANCE_NUMBER");}
 public CsvCell getPasId() { return super.getCell( "PAS_ID");}
 public CsvCell getHospitalName() { return super.getCell( "HOSPITAL_NAME");}
 public CsvCell getHospitalCode() { return super.getCell( "HOSPITAL_CODE");}
 public CsvCell getHospitalSite() { return super.getCell( "HOSPITAL_SITE");}
 public CsvCell getAttendanceType() { return super.getCell( "ATTENDANCE_TYPE");}
 public CsvCell getFirstLocation() { return super.getCell( "FIRST_LOCATION");}
 public CsvCell getLastLocation() { return super.getCell( "LAST_LOCATION");}
 public CsvCell getArrivalMode() { return super.getCell( "ARRIVAL_MODE");}
 public CsvCell getReferralSource() { return super.getCell( "REFERRAL_SOURCE");}
 public CsvCell getArrivalDttm() { return super.getCell( "ARRIVAL_DTTM");}
 public CsvCell getRegistrationDttm() { return super.getCell( "REGISTRATION_DTTM");}
 public CsvCell getRatDttm() { return super.getCell( "RAT_DTTM");}
 public CsvCell getTriageDttm() { return super.getCell( "TRIAGE_DTTM");}
 public CsvCell getReferredAeDoctorDttm() { return super.getCell( "REFERRED_AE_DOCTOR_DTTM");}
 public CsvCell getSeenByAeDoctorDttm() { return super.getCell( "SEEN_BY_AE_DOCTOR_DTTM");}
 public CsvCell getReferredToPathologyDttm() { return super.getCell( "REFERRED_TO_PATHOLOGY_DTTM");}
 public CsvCell getBackFromPathologyDttm() { return super.getCell( "BACK_FROM_PATHOLOGY_DTTM");}
 public CsvCell getReferredToXrayDttm() { return super.getCell( "REFERRED_TO_XRAY_DTTM");}
 public CsvCell getBackFromXrayDttm() { return super.getCell( "BACK_FROM_XRAY_DTTM");}
 public CsvCell getReferredToTreatmentDttm() { return super.getCell( "REFERRED_TO_TREATMENT_DTTM");}
 public CsvCell getBackFromTreatmentDttm() { return super.getCell( "BACK_FROM_TREATMENT_DTTM");}
 public CsvCell getFirstReferredToSpecialtyDttm() { return super.getCell( "FIRST_REFERRED_TO_SPECIALTY_DTTM");}
 public CsvCell getFirstSeenBySpecialtyDttm() { return super.getCell( "FIRST_SEEN_BY_SPECIALTY_DTTM");}
 public CsvCell getFirstSpecialtyReferred() { return super.getCell( "FIRST_SPECIALTY_REFERRED");}
 public CsvCell getLastReferredToSpecialtyDttm() { return super.getCell( "LAST_REFERRED_TO_SPECIALTY_DTTM");}
 public CsvCell getLastSeenBySpecialtyDttm() { return super.getCell( "LAST_SEEN_BY_SPECIALTY_DTTM");}
 public CsvCell getLastSpecialtyReferred() { return super.getCell( "LAST_SPECIALTY_REFERRED");}
 public CsvCell getBedRequestDttm() { return super.getCell( "BED_REQUEST_DTTM");}
 public CsvCell getBedRequestOutcomeDttm() { return super.getCell( "BED_REQUEST_OUTCOME_DTTM");}
 public CsvCell getMinutesToBedRequest() { return super.getCell( "MINUTES_TO_BED_REQUEST");}
 public CsvCell getCauBedRequestDttm() { return super.getCell( "CAU_BED_REQUEST_DTTM");}
 public CsvCell getMauBedRequestDttm() { return super.getCell( "MAU_BED_REQUEST_DTTM");}
 public CsvCell getSauBedRequestDttm() { return super.getCell( "SAU_BED_REQUEST_DTTM");}
 public CsvCell getTransportRequestDttm() { return super.getCell( "TRANSPORT_REQUEST_DTTM");}
 public CsvCell getTransportOutcomeDttm() { return super.getCell( "TRANSPORT_OUTCOME_DTTM");}
 public CsvCell getDiagnosisRecordDttm() { return super.getCell( "DIAGNOSIS_RECORD_DTTM");}
 public CsvCell getCliniciansName() { return super.getCell( "CLINICIANS_NAME");}
 public CsvCell getComplaint() { return super.getCell( "COMPLAINT");}
 public CsvCell getDischargedDttm() { return super.getCell( "DISCHARGED_DTTM");}
 public CsvCell getLeftDepartmentDttm() { return super.getCell( "LEFT_DEPARTMENT_DTTM");}
 public CsvCell getDischargeDestination() { return super.getCell( "DISCHARGE_DESTINATION");}
 public CsvCell getRecordedOutcome() { return super.getCell( "RECORDED_OUTCOME");}



protected String getFileTypeDescription() {return "bhrutAandeAttendances Entry file ";}

    @Override
protected boolean isFileAudited() {return true;}
        }
