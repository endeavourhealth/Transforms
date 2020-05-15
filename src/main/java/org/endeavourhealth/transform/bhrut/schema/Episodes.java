package org.endeavourhealth.transform.bhrut.schema;

import org.endeavourhealth.transform.bhrut.BhrutCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class Episodes extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(Episodes.class); 

  public Episodes(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                       "IP_SPELL_EXTERNAL_ID",
                       "ADM_EPI_FLAG",
                       "DIS_EPI_FLAG",
                       "PATHWAY_ID",
                       "PAS_ID",
                       "EPI_NUM",
                       "EPISODE_CONSULTANT_CODE",
                       "EPISODE_CONSULTANT",
                       "EPISODE_SPECIALTY_CODE",
                       "EPISODE_SPECIALTY",
                       "EPISODE_START_WARD_CODE",
                       "EPISODE_START_WARD",
                       "EPISODE_END_WARD_CODE",
                       "EPISODE_END_WARD",
                       "EPISODE_START_DTTM",
                       "EPISODE_END_DTTM",
                       "POINT_OF_DELIVERY",
                       "ADMINISTRATIVE_CATEGORY_CODE",
                       "ADMINISTRATIVE_CATEGORY",
                       "ADMISSION_METHOD_CODE",
                       "ADMISSION_METHOD",
                       "ADMISSION_SOURCE_CODE",
                       "ADMISSION_SOURCE",
                       "PATIENT_CLASS_CODE",
                       "PATIENT_CLASS",
                       "INTENDED_MANAGEMENT_CODE",
                       "INTENDED_MANAGEMENT",
                       "DISCHARGE_METHOD_CODE",
                       "DISCHARGE_METHOD",
                       "DISCHARGE_DESINATION_CODE",
                       "DISCHARGE_DESTINATION",
                       "ADMISSION_HOSPITAL_CODE",
                       "ADMISSION_HOSPITAL_NAME",
                       "PRIMARY_DIAGNOSIS_CODE",
                       "PRIMDIAG_DTTM",
                       "DIAG_01",
                       "DIAG1_DTTM",
                       "DIAG_02",
                       "DIAG2_DTTM",
                       "DIAG_03",
                       "DIAG3_DTTM",
                       "DIAG_04",
                       "DIAG4_DTTM",
                       "DIAG_05",
                       "DIAG5_DTTM",
                       "DIAG_06",
                       "DIAG6_DTTM",
                       "DIAG_07",
                       "DIAG7_DTTM",
                       "DIAG_08",
                       "DIAG8_DTTM",
                       "DIAG_09",
                       "DIAG9_DTTM",
                       "DIAG_10",
                       "DIAG10_DTTM",
                       "DIAG_11",
                       "DIAG11_DTTM",
                       "DIAG_12",
                       "DIAG12_DTTM",
                       "PRIMARY_PROCEDURE_CODE",
                       "PRIMARY_PROCEDURE_DATE",
                       "PRIMARY_PROCEDURE",
                       "PROC_01",
                       "PROC_01_DESC",
                       "PROC_02",
                       "PROC_02_DESC",
                       "PROC_03",
                       "PROC_03_DESC",
                       "PROC_04",
                       "PROC_05",
                       "PROC_06",
                       "PROC_07",
                       "PROC_08",
                       "PROC_09",
                       "PROC_10",
                       "PROC_11",
                       "PROC_12"
                    

            };

        }
 public CsvCell getLinestatus() { return super.getCell("LineStatus");}
 public CsvCell getId() { return super.getCell( "EXTERNAL_ID");}
 public CsvCell getIpSpellExternalId() { return super.getCell( "IP_SPELL_EXTERNAL_ID");}
 public CsvCell getAdmEpiFlag() { return super.getCell( "ADM_EPI_FLAG");}
 public CsvCell getDisEpiFlag() { return super.getCell( "DIS_EPI_FLAG");}
 public CsvCell getPathwayId() { return super.getCell( "PATHWAY_ID");}
 public CsvCell getPasId() { return super.getCell( "PAS_ID");}
 public CsvCell getEpiNum() { return super.getCell( "EPI_NUM");}
 public CsvCell getEpisodeConsultantCode() { return super.getCell( "EPISODE_CONSULTANT_CODE");}
 public CsvCell getEpisodeConsultant() { return super.getCell( "EPISODE_CONSULTANT");}
 public CsvCell getEpisodeSpecialtyCode() { return super.getCell( "EPISODE_SPECIALTY_CODE");}
 public CsvCell getEpisodeSpecialty() { return super.getCell( "EPISODE_SPECIALTY");}
 public CsvCell getEpisodeStartWardCode() { return super.getCell( "EPISODE_START_WARD_CODE");}
 public CsvCell getEpisodeStartWard() { return super.getCell( "EPISODE_START_WARD");}
 public CsvCell getEpisodeEndWardCode() { return super.getCell( "EPISODE_END_WARD_CODE");}
 public CsvCell getEpisodeEndWard() { return super.getCell( "EPISODE_END_WARD");}
 public CsvCell getEpisodeStartDttm() { return super.getCell( "EPISODE_START_DTTM");}
 public CsvCell getEpisodeEndDttm() { return super.getCell( "EPISODE_END_DTTM");}
 public CsvCell getPointOfDelivery() { return super.getCell( "POINT_OF_DELIVERY");}
 public CsvCell getAdministrativeCategoryCode() { return super.getCell( "ADMINISTRATIVE_CATEGORY_CODE");}
 public CsvCell getAdministrativeCategory() { return super.getCell( "ADMINISTRATIVE_CATEGORY");}
 public CsvCell getAdmissionMethodCode() { return super.getCell( "ADMISSION_METHOD_CODE");}
 public CsvCell getAdmissionMethod() { return super.getCell( "ADMISSION_METHOD");}
 public CsvCell getAdmissionSourceCode() { return super.getCell( "ADMISSION_SOURCE_CODE");}
 public CsvCell getAdmissionSource() { return super.getCell( "ADMISSION_SOURCE");}
 public CsvCell getPatientClassCode() { return super.getCell( "PATIENT_CLASS_CODE");}
 public CsvCell getPatientClass() { return super.getCell( "PATIENT_CLASS");}
 public CsvCell getIntendedManagementCode() { return super.getCell( "INTENDED_MANAGEMENT_CODE");}
 public CsvCell getIntendedManagement() { return super.getCell( "INTENDED_MANAGEMENT");}
 public CsvCell getDischargeMethodCode() { return super.getCell( "DISCHARGE_METHOD_CODE");}
 public CsvCell getDischargeMethod() { return super.getCell( "DISCHARGE_METHOD");}
 public CsvCell getDischargeDesinationCode() { return super.getCell( "DISCHARGE_DESINATION_CODE");}
 public CsvCell getDischargeDestination() { return super.getCell( "DISCHARGE_DESTINATION");}
 public CsvCell getAdmissionHospitalCode() { return super.getCell( "ADMISSION_HOSPITAL_CODE");}
 public CsvCell getAdmissionHospitalName() { return super.getCell( "ADMISSION_HOSPITAL_NAME");}
 public CsvCell getPrimaryDiagnosisCode() { return super.getCell( "PRIMARY_DIAGNOSIS_CODE");}
 public CsvCell getPrimdiagDttm() { return super.getCell( "PRIMDIAG_DTTM");}
 public CsvCell getDiag1() { return super.getCell( "DIAG_01");}
 public CsvCell getDiag1Dttm() { return super.getCell( "DIAG1_DTTM");}
 public CsvCell getDiag2() { return super.getCell( "DIAG_02");}
 public CsvCell getDiag2Dttm() { return super.getCell( "DIAG2_DTTM");}
 public CsvCell getDiag3() { return super.getCell( "DIAG_03");}
 public CsvCell getDiag3Dttm() { return super.getCell( "DIAG3_DTTM");}
 public CsvCell getDiag4() { return super.getCell( "DIAG_04");}
 public CsvCell getDiag4Dttm() { return super.getCell( "DIAG4_DTTM");}
 public CsvCell getDiag5() { return super.getCell( "DIAG_05");}
 public CsvCell getDiag5Dttm() { return super.getCell( "DIAG5_DTTM");}
 public CsvCell getDiag6() { return super.getCell( "DIAG_06");}
 public CsvCell getDiag6Dttm() { return super.getCell( "DIAG6_DTTM");}
 public CsvCell getDiag7() { return super.getCell( "DIAG_07");}
 public CsvCell getDiag7Dttm() { return super.getCell( "DIAG7_DTTM");}
 public CsvCell getDiag8() { return super.getCell( "DIAG_08");}
 public CsvCell getDiag8Dttm() { return super.getCell( "DIAG8_DTTM");}
 public CsvCell getDiag9() { return super.getCell( "DIAG_09");}
 public CsvCell getDiag9Dttm() { return super.getCell( "DIAG9_DTTM");}
 public CsvCell getDiag10() { return super.getCell( "DIAG_10");}
 public CsvCell getDiag10Dttm() { return super.getCell( "DIAG10_DTTM");}
 public CsvCell getDiag11() { return super.getCell( "DIAG_11");}
 public CsvCell getDiag11Dttm() { return super.getCell( "DIAG11_DTTM");}
 public CsvCell getDiag12() { return super.getCell( "DIAG_12");}
 public CsvCell getDiag12Dttm() { return super.getCell( "DIAG12_DTTM");}
 public CsvCell getPrimaryProcedureCode() { return super.getCell( "PRIMARY_PROCEDURE_CODE");}
 public CsvCell getPrimaryProcedureDate() { return super.getCell( "PRIMARY_PROCEDURE_DATE");}
 public CsvCell getPrimaryProcedure() { return super.getCell( "PRIMARY_PROCEDURE");}
 public CsvCell getProc1() { return super.getCell( "PROC_01");}
 public CsvCell getProc1Desc() { return super.getCell( "PROC_01_DESC");}
 public CsvCell getProc2() { return super.getCell( "PROC_02");}
 public CsvCell getProc2Desc() { return super.getCell( "PROC_02_DESC");}
 public CsvCell getProc3() { return super.getCell( "PROC_03");}
 public CsvCell getProc3Desc() { return super.getCell( "PROC_03_DESC");}
 public CsvCell getProc4() { return super.getCell( "PROC_04");}
 public CsvCell getProc5() { return super.getCell( "PROC_05");}
 public CsvCell getProc6() { return super.getCell( "PROC_06");}
 public CsvCell getProc7() { return super.getCell( "PROC_07");}
 public CsvCell getProc8() { return super.getCell( "PROC_08");}
 public CsvCell getProc9() { return super.getCell( "PROC_09");}
 public CsvCell getProc10() { return super.getCell( "PROC_10");}
 public CsvCell getProc11() { return super.getCell( "PROC_11");}
 public CsvCell getProc12() { return super.getCell( "PROC_12");}



protected String getFileTypeDescription() {return "bhrutEpisodes Entry file ";}

    @Override
protected boolean isFileAudited() {return true;}
        }
