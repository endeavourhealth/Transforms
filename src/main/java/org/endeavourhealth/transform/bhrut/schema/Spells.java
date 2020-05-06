package org.endeavourhealth.transform.bhrut.schema;

import org.endeavourhealth.transform.bhrut.BhrutCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class Spells extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(Spells.class); 

  public Spells(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                       "PAS_ID",
                       "ADMISSION_DTTM",
                       "ADMISSION_CONSULTANT",
                       "ADMISSION_CONSULTANT_CODE",
                       "ADMISSION_CONSULTANT_MAIN_SPECIALTY_CODE",
                       "ADMISSION_HOSPITAL_NAME",
                       "ADMISSION_HOSPITAL_CODE",
                       "ADMISSION_METHOD",
                       "ADMISSION_METHOD_CODE",
                       "ADMISSION_SOURCE",
                       "ADMISSION_SOURCE_CODE",
                       "ADMISSION_SPECIALTY",
                       "ADMISSION_SPECIALTY_CODE",
                       "ADMISSION_WARD",
                       "ADMISSION_WARD_CODE",
                       "REGULAR_DAY_NIGHT",
                       "REGULAR_DAY_NIGHT_CODE",
                       "INTENDED_MANAGEMENT",
                       "INTENDED_MANAGEMENT_CODE",
                       "PATIENT_CLASS",
                       "PATIENT_CLASS_CODE",
                       "POINT_OF_DELIVERY",
                       "MEDICAL_DISCHARGE_DTTM",
                       "VTE_ASSESSMENT",
                       "DISCHARGE_DTTM",
                       "DISCHARGE_METHOD",
                       "DISCHARGE_METHOD_CODE",
                       "DISCHARGE_DESTINATION",
                       "DISCHARGE_DESTINATION_CODE",
                       "DISCHARGE_CONSULTANT",
                       "DISCHARGE_CONSULTANT_CODE",
                       "DISCHARGE_CONSULTANT_MAIN_SPECIALTY",
                       "DISCHARGE_CONSULTANT_MAIN_SPECIALTY_CODE",
                       "DISCHARGE_HOSPITAL_NAME",
                       "DISCHARGE_HOSPITAL_CODE",
                       "DISCHARGE_SPECIALTY",
                       "DISCHARGE_SPECIALTY_CODE",
                       "DISCHARGE_WARD",
                       "DISCHARGE_WARD_CODE",
                       "TRANSFERRED_TO_DISCHARGE_LOUNGE_DTTM",
                       "DISCHARGE_LOCATION",
                       "DISCHARGE_LOCATION_CODE",
                       "PRIMARY_DIAGNOSIS",
                       "PRIMARY_DIAGNOSIS_CODE",
                       "PRIMARY_PROCEDURE",
                       "PRIMARY_PROCEDURE_CODE",
                       "REFERRING_CONSULTANT",
                       "SPELL_REGISTERED_GP_CODE",
                       "SPELL_REGISTERED_GP_PRACTICE",
                       "SPELL_REGISTERED_GP_PRACTICE_CODE",
                       "SPELL_CCG",
                       "PATHWAY_ID",
                       "DAYS_SINCE_DISCHARGE",
                       "READMISSION_IPSP_EXT_ID",
                       "WLENTRY_EXTERNAL_ID"
                    

            };

        }
 public CsvCell getLinestatus() { return super.getCell("LineStatus");}
 public CsvCell getId() { return super.getCell( "EXTERNAL_ID");}
 public CsvCell getPasId() { return super.getCell( "PAS_ID");}
 public CsvCell getAdmissionDttm() { return super.getCell( "ADMISSION_DTTM");}
 public CsvCell getAdmissionConsultant() { return super.getCell( "ADMISSION_CONSULTANT");}
 public CsvCell getAdmissionConsultantCode() { return super.getCell( "ADMISSION_CONSULTANT_CODE");}
 public CsvCell getAdmissionConsultantMainSpecialtyCode() { return super.getCell( "ADMISSION_CONSULTANT_MAIN_SPECIALTY_CODE");}
 public CsvCell getAdmissionHospitalName() { return super.getCell( "ADMISSION_HOSPITAL_NAME");}
 public CsvCell getAdmissionHospitalCode() { return super.getCell( "ADMISSION_HOSPITAL_CODE");}
 public CsvCell getAdmissionMethod() { return super.getCell( "ADMISSION_METHOD");}
 public CsvCell getAdmissionMethodCode() { return super.getCell( "ADMISSION_METHOD_CODE");}
 public CsvCell getAdmissionSource() { return super.getCell( "ADMISSION_SOURCE");}
 public CsvCell getAdmissionSourceCode() { return super.getCell( "ADMISSION_SOURCE_CODE");}
 public CsvCell getAdmissionSpecialty() { return super.getCell( "ADMISSION_SPECIALTY");}
 public CsvCell getAdmissionSpecialtyCode() { return super.getCell( "ADMISSION_SPECIALTY_CODE");}
 public CsvCell getAdmissionWard() { return super.getCell( "ADMISSION_WARD");}
 public CsvCell getAdmissionWardCode() { return super.getCell( "ADMISSION_WARD_CODE");}
 public CsvCell getRegularDayNight() { return super.getCell( "REGULAR_DAY_NIGHT");}
 public CsvCell getRegularDayNightCode() { return super.getCell( "REGULAR_DAY_NIGHT_CODE");}
 public CsvCell getIntendedManagement() { return super.getCell( "INTENDED_MANAGEMENT");}
 public CsvCell getIntendedManagementCode() { return super.getCell( "INTENDED_MANAGEMENT_CODE");}
 public CsvCell getPatientClass() { return super.getCell( "PATIENT_CLASS");}
 public CsvCell getPatientClassCode() { return super.getCell( "PATIENT_CLASS_CODE");}
 public CsvCell getPointOfDelivery() { return super.getCell( "POINT_OF_DELIVERY");}
 public CsvCell getMedicalDischargeDttm() { return super.getCell( "MEDICAL_DISCHARGE_DTTM");}
 public CsvCell getVteAssessment() { return super.getCell( "VTE_ASSESSMENT");}
 public CsvCell getDischargeDttm() { return super.getCell( "DISCHARGE_DTTM");}
 public CsvCell getDischargeMethod() { return super.getCell( "DISCHARGE_METHOD");}
 public CsvCell getDischargeMethodCode() { return super.getCell( "DISCHARGE_METHOD_CODE");}
 public CsvCell getDischargeDestination() { return super.getCell( "DISCHARGE_DESTINATION");}
 public CsvCell getDischargeDestinationCode() { return super.getCell( "DISCHARGE_DESTINATION_CODE");}
 public CsvCell getDischargeConsultant() { return super.getCell( "DISCHARGE_CONSULTANT");}
 public CsvCell getDischargeConsultantCode() { return super.getCell( "DISCHARGE_CONSULTANT_CODE");}
 public CsvCell getDischargeConsultantMainSpecialty() { return super.getCell( "DISCHARGE_CONSULTANT_MAIN_SPECIALTY");}
 public CsvCell getDischargeConsultantMainSpecialtyCode() { return super.getCell( "DISCHARGE_CONSULTANT_MAIN_SPECIALTY_CODE");}
 public CsvCell getDischargeHospitalName() { return super.getCell( "DISCHARGE_HOSPITAL_NAME");}
 public CsvCell getDischargeHospitalCode() { return super.getCell( "DISCHARGE_HOSPITAL_CODE");}
 public CsvCell getDischargeSpecialty() { return super.getCell( "DISCHARGE_SPECIALTY");}
 public CsvCell getDischargeSpecialtyCode() { return super.getCell( "DISCHARGE_SPECIALTY_CODE");}
 public CsvCell getDischargeWard() { return super.getCell( "DISCHARGE_WARD");}
 public CsvCell getDischargeWardCode() { return super.getCell( "DISCHARGE_WARD_CODE");}
 public CsvCell getTransferredToDischargeLoungeDttm() { return super.getCell( "TRANSFERRED_TO_DISCHARGE_LOUNGE_DTTM");}
 public CsvCell getDischargeLocation() { return super.getCell( "DISCHARGE_LOCATION");}
 public CsvCell getDischargeLocationCode() { return super.getCell( "DISCHARGE_LOCATION_CODE");}
 public CsvCell getPrimaryDiagnosis() { return super.getCell( "PRIMARY_DIAGNOSIS");}
 public CsvCell getPrimaryDiagnosisCode() { return super.getCell( "PRIMARY_DIAGNOSIS_CODE");}
 public CsvCell getPrimaryProcedure() { return super.getCell( "PRIMARY_PROCEDURE");}
 public CsvCell getPrimaryProcedureCode() { return super.getCell( "PRIMARY_PROCEDURE_CODE");}
 public CsvCell getReferringConsultant() { return super.getCell( "REFERRING_CONSULTANT");}
 public CsvCell getSpellRegisteredGpCode() { return super.getCell( "SPELL_REGISTERED_GP_CODE");}
 public CsvCell getSpellRegisteredGpPractice() { return super.getCell( "SPELL_REGISTERED_GP_PRACTICE");}
 public CsvCell getSpellRegisteredGpPracticeCode() { return super.getCell( "SPELL_REGISTERED_GP_PRACTICE_CODE");}
 public CsvCell getSpellCcg() { return super.getCell( "SPELL_CCG");}
 public CsvCell getPathwayId() { return super.getCell( "PATHWAY_ID");}
 public CsvCell getDaysSinceDischarge() { return super.getCell( "DAYS_SINCE_DISCHARGE");}
 public CsvCell getReadmissionIpspExtId() { return super.getCell( "READMISSION_IPSP_EXT_ID");}
 public CsvCell getWlentryExternalId() { return super.getCell( "WLENTRY_EXTERNAL_ID");}



protected String getFileTypeDescription() {return "bhrutSpells Entry file ";}

    @Override
protected boolean isFileAudited() {return true;}
        }
