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
                BhrutCsvToFhirTransformer.CSV_FORMAT.withHeader(getHeaders(version)),
                BhrutCsvToFhirTransformer.DATE_FORMAT,
                BhrutCsvToFhirTransformer.TIME_FORMAT);
    }


    @Override
    protected String[] getCsvHeaders(String version) {
        return getHeaders(version);
    }
    private static String[] getHeaders(String version) {
        return new String[]{
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
                "PRIMARY_DIAGNOSIS_CODING_TYPE",
                "DIAG_01",
                "DIAG1_DTTM",
                "DIAG1_CODING_TYPE",
                "DIAG_02",
                "DIAG2_DTTM",
                "DIAG2_CODING_TYPE",
                "DIAG_03",
                "DIAG3_DTTM",
                "DIAG3_CODING_TYPE",
                "DIAG_04",
                "DIAG4_DTTM",
                "DIAG4_CODING_TYPE",
                "DIAG_05",
                "DIAG5_DTTM",
                "DIAG5_CODING_TYPE",
                "DIAG_06",
                "DIAG6_DTTM",
                "DIAG6_CODING_TYPE",
                "DIAG_07",
                "DIAG7_DTTM",
                "DIAG7_CODING_TYPE",
                "DIAG_08",
                "DIAG8_DTTM",
                "DIAG8_CODING_TYPE",
                "DIAG_09",
                "DIAG9_DTTM",
                "DIAG9_CODING_TYPE",
                "DIAG_10",
                "DIAG10_DTTM",
                "DIAG10_CODING_TYPE",
                "DIAG_11",
                "DIAG11_DTTM",
                "DIAG11_CODING_TYPE",
                "DIAG_12",
                "DIAG12_DTTM",
                "DIAG12_CODING_TYPE",
                "PRIMARY_PROCEDURE_CODE",
                "PRIMARY_PROCEDURE_DATE",
                "PRIMARY_PROCEDURE",
                "PRIMARY_PROCEDURE_CODING_TYPE",
                "PROC_01",
                "PROC_01_DESC",
                "PROC_01_CODING_TYPE",
                "PROC_02",
                "PROC_02_DESC",
                "PROC_02_CODING_TYPE",
                "PROC_03",
                "PROC_03_DESC",
                "PROC_03_CODING_TYPE",
                "PROC_04",
                "PROC_04_DESC",
                "PROC_04_CODING_TYPE",
                "PROC_05",
                "PROC_05_DESC",
                "PROC_05_CODING_TYPE",
                "PROC_06",
                "PROC_06_DESC",
                "PROC_06_CODING_TYPE",
                "PROC_07",
                "PROC_07_DESC",
                "PROC_07_CODING_TYPE",
                "PROC_08",
                "PROC_08_DESC",
                "PROC_08_CODING_TYPE",
                "PROC_09",
                "PROC_09_DESC",
                "PROC_09_CODING_TYPE",
                "PROC_10",
                "PROC_10_DESC",
                "PROC_10_CODING_TYPE",
                "PROC_11",
                "PROC_11_DESC",
                "PROC_11_CODING_TYPE",
                "PROC_12",
                "PROC_12_DESC",
                "PROC_12_CODING_TYPE",
                "DataUpdateStatus"


        };

    }

    public CsvCell getId() {
        CsvCell id = super.getCell("EXTERNAL_ID");
        String newId = "BHRUT-" + id.getString();
        CsvCell ret = new CsvCell(id.getPublishedFileId(), id.getRecordNumber(), id.getColIndex(), newId, id.getParentParser());
        return ret;
    }

    public CsvCell getIpSpellExternalId() {
        CsvCell id = super.getCell("IP_SPELL_EXTERNAL_ID");
        String newId = "BHRUT-" + id.getString();
        CsvCell ret = new CsvCell(id.getPublishedFileId(), id.getRecordNumber(), id.getColIndex(), newId, id.getParentParser());
        return ret;
    }

    public CsvCell getAdmEpiFlag() {
        return super.getCell("ADM_EPI_FLAG");
    }

    public CsvCell getDisEpiFlag() {
        return super.getCell("DIS_EPI_FLAG");
    }

    public CsvCell getPathwayId() {
        return super.getCell("PATHWAY_ID");
    }

    public CsvCell getPasId() {
        return super.getCell("PAS_ID");
    }

    public CsvCell getEpiNum() {
        return super.getCell("EPI_NUM");
    }

    public CsvCell getEpisodeConsultantCode() {
        return super.getCell("EPISODE_CONSULTANT_CODE");
    }

    public CsvCell getEpisodeConsultant() {
        return super.getCell("EPISODE_CONSULTANT");
    }

    public CsvCell getEpisodeSpecialtyCode() {
        return super.getCell("EPISODE_SPECIALTY_CODE");
    }

    public CsvCell getEpisodeSpecialty() {
        return super.getCell("EPISODE_SPECIALTY");
    }

    public CsvCell getEpisodeStartWardCode() {
        return super.getCell("EPISODE_START_WARD_CODE");
    }

    public CsvCell getEpisodeStartWard() {
        return super.getCell("EPISODE_START_WARD");
    }

    public CsvCell getEpisodeEndWardCode() {
        return super.getCell("EPISODE_END_WARD_CODE");
    }

    public CsvCell getEpisodeEndWard() {
        return super.getCell("EPISODE_END_WARD");
    }

    public CsvCell getEpisodeStartDttm() {
        return super.getCell("EPISODE_START_DTTM");
    }

    public CsvCell getEpisodeEndDttm() {
        return super.getCell("EPISODE_END_DTTM");
    }

    public CsvCell getPointOfDelivery() {
        return super.getCell("POINT_OF_DELIVERY");
    }

    public CsvCell getAdministrativeCategoryCode() {
        return super.getCell("ADMINISTRATIVE_CATEGORY_CODE");
    }

    public CsvCell getAdministrativeCategory() {
        return super.getCell("ADMINISTRATIVE_CATEGORY");
    }

    public CsvCell getAdmissionMethodCode() {
        return super.getCell("ADMISSION_METHOD_CODE");
    }

    public CsvCell getAdmissionMethod() {
        return super.getCell("ADMISSION_METHOD");
    }

    public CsvCell getAdmissionSourceCode() {
        return super.getCell("ADMISSION_SOURCE_CODE");
    }

    public CsvCell getAdmissionSource() {
        return super.getCell("ADMISSION_SOURCE");
    }

    public CsvCell getPatientClassCode() {
        return super.getCell("PATIENT_CLASS_CODE");
    }

    public CsvCell getPatientClass() {
        return super.getCell("PATIENT_CLASS");
    }

    public CsvCell getIntendedManagementCode() {
        return super.getCell("INTENDED_MANAGEMENT_CODE");
    }

    public CsvCell getIntendedManagement() {
        return super.getCell("INTENDED_MANAGEMENT");
    }

    public CsvCell getDischargeMethodCode() {
        return super.getCell("DISCHARGE_METHOD_CODE");
    }

    public CsvCell getDischargeMethod() {
        return super.getCell("DISCHARGE_METHOD");
    }

    public CsvCell getDischargeDestinationCode() {
        return super.getCell("DISCHARGE_DESINATION_CODE");
    }

    public CsvCell getDischargeDestination() {
        return super.getCell("DISCHARGE_DESTINATION");
    }

    public CsvCell getAdmissionHospitalCode() {
        return super.getCell("ADMISSION_HOSPITAL_CODE");
    }

    public CsvCell getAdmissionHospitalName() {
        return super.getCell("ADMISSION_HOSPITAL_NAME");
    }

    public CsvCell getPrimaryDiagnosisCode() {
        return super.getCell("PRIMARY_DIAGNOSIS_CODE");
    }

    public CsvCell getPrimaryDiagnosisCodingType() {
        return super.getCell("PRIMARY_DIAGNOSIS_CODING_TYPE");
    }

    public CsvCell getPrimdiagDttm() {
        return super.getCell("PRIMDIAG_DTTM");
    }

    public CsvCell getDiag1() {
        return super.getCell("DIAG_01");
    }

    public CsvCell getDiag1Dttm() {
        return super.getCell("DIAG1_DTTM");
    }

    public CsvCell getDiag1CodingType() {
        return super.getCell("DIAG1_CODING_TYPE");
    }

    public CsvCell getDiag2() {
        return super.getCell("DIAG_02");
    }

    public CsvCell getDiag2Dttm() {
        return super.getCell("DIAG2_DTTM");
    }

    public CsvCell getDiag2CodingType() {
        return super.getCell("DIAG2_CODING_TYPE");
    }

    public CsvCell getDiag3() {
        return super.getCell("DIAG_03");
    }

    public CsvCell getDiag3Dttm() {
        return super.getCell("DIAG3_DTTM");
    }

    public CsvCell getDiag3CodingType() {
        return super.getCell("DIAG3_CODING_TYPE");
    }

    public CsvCell getDiag4() {
        return super.getCell("DIAG_04");
    }

    public CsvCell getDiag4Dttm() {
        return super.getCell("DIAG4_DTTM");
    }

    public CsvCell getDiag4CodingType() {
        return super.getCell("DIAG4_CODING_TYPE");
    }

    public CsvCell getDiag5() {
        return super.getCell("DIAG_05");
    }

    public CsvCell getDiag5Dttm() {
        return super.getCell("DIAG5_DTTM");
    }

    public CsvCell getDiag5CodingType() {
        return super.getCell("DIAG5_CODING_TYPE");
    }

    public CsvCell getDiag6() {
        return super.getCell("DIAG_06");
    }

    public CsvCell getDiag6Dttm() {
        return super.getCell("DIAG6_DTTM");
    }

    public CsvCell getDiag6CodingType() {
        return super.getCell("DIAG6_CODING_TYPE");
    }

    public CsvCell getDiag7() {
        return super.getCell("DIAG_07");
    }

    public CsvCell getDiag7Dttm() {
        return super.getCell("DIAG7_DTTM");
    }

    public CsvCell getDiag7CodingType() {
        return super.getCell("DIAG7_CODING_TYPE");
    }

    public CsvCell getDiag8() {
        return super.getCell("DIAG_08");
    }

    public CsvCell getDiag8Dttm() {
        return super.getCell("DIAG8_DTTM");
    }

    public CsvCell getDiag8CodingType() {
        return super.getCell("DIAG8_CODING_TYPE");
    }

    public CsvCell getDiag9() {
        return super.getCell("DIAG_09");
    }

    public CsvCell getDiag9Dttm() {
        return super.getCell("DIAG9_DTTM");
    }

    public CsvCell getDiag9CodingType() {
        return super.getCell("DIAG9_CODING_TYPE");
    }

    public CsvCell getDiag10() {
        return super.getCell("DIAG_10");
    }

    public CsvCell getDiag10Dttm() {
        return super.getCell("DIAG10_DTTM");
    }

    public CsvCell getDiag10CodingType() {
        return super.getCell("DIAG10_CODING_TYPE");
    }

    public CsvCell getDiag11() {
        return super.getCell("DIAG_11");
    }

    public CsvCell getDiag11Dttm() {
        return super.getCell("DIAG11_DTTM");
    }

    public CsvCell getDiag11CodingType() {
        return super.getCell("DIAG11_CODING_TYPE");
    }

    public CsvCell getDiag12() {
        return super.getCell("DIAG_12");
    }

    public CsvCell getDiag12Dttm() {
        return super.getCell("DIAG12_DTTM");
    }

    public CsvCell getDiag12CodingType() {
        return super.getCell("DIAG12_CODING_TYPE");
    }

    public CsvCell getPrimaryProcedureCode() {
        return super.getCell("PRIMARY_PROCEDURE_CODE");
    }

    public CsvCell getPrimaryProcedureCodingType() {
        return super.getCell("PRIMARY_PROCEDURE_CODING_TYPE");
    }

    public CsvCell getPrimaryProcedureDate() {
        return super.getCell("PRIMARY_PROCEDURE_DATE");
    }

    public CsvCell getPrimaryProcedure() {
        return super.getCell("PRIMARY_PROCEDURE");
    }

    public CsvCell getProc1() {
        return super.getCell("PROC_01");
    }

    public CsvCell getProc1Description() {
        return super.getCell("PROC_01_DESC");
    }

    public CsvCell getProc1CodingType() {
        return super.getCell("PROC_01_CODING_TYPE");
    }

    public CsvCell getProc2() {
        return super.getCell("PROC_02");
    }

    public CsvCell getProc2Description() {
        return super.getCell("PROC_02_DESC");
    }

    public CsvCell getProc2CodingType() {
        return super.getCell("PROC_02_CODING_TYPE");
    }

    public CsvCell getProc3() {
        return super.getCell("PROC_03");
    }

    public CsvCell getProc3Description() {
        return super.getCell("PROC_03_DESC");
    }

    public CsvCell getProc3CodingType() {
        return super.getCell("PROC_03_CODING_TYPE");
    }

    public CsvCell getProc4() {
        return super.getCell("PROC_04");
    }

    public CsvCell getProc4Description() {
        return super.getCell("PROC_04_DESC");
    }

    public CsvCell getProc4CodingType() {
        return super.getCell("PROC_04_CODING_TYPE");
    }

    public CsvCell getProc5() {
        return super.getCell("PROC_05");
    }

    public CsvCell getProc5Description() {
        return super.getCell("PROC_05_DESC");
    }

    public CsvCell getProc5CodingType() {
        return super.getCell("PROC_05_CODING_TYPE");
    }

    public CsvCell getProc6() {
        return super.getCell("PROC_06");
    }

    public CsvCell getProc6Description() {
        return super.getCell("PROC_06_DESC");
    }

    public CsvCell getProc6CodingType() {
        return super.getCell("PROC_06_CODING_TYPE");
    }

    public CsvCell getProc7() {
        return super.getCell("PROC_07");
    }

    public CsvCell getProc7Description() {
        return super.getCell("PROC_07_DESC");
    }

    public CsvCell getProc7CodingType() {
        return super.getCell("PROC_07_CODING_TYPE");
    }

    public CsvCell getProc8() {
        return super.getCell("PROC_08");
    }

    public CsvCell getProc8Description() {
        return super.getCell("PROC_08_DESC");
    }

    public CsvCell getProc8CodingType() {
        return super.getCell("PROC_08_CODING_TYPE");
    }

    public CsvCell getProc9() {
        return super.getCell("PROC_09");
    }

    public CsvCell getProc9Description() {
        return super.getCell("PROC_09_DESC");
    }

    public CsvCell getProc9CodingType() {
        return super.getCell("PROC_09_CODING_TYPE");
    }

    public CsvCell getProc10() {
        return super.getCell("PROC_10");
    }

    public CsvCell getProc10Description() {
        return super.getCell("PROC_10_DESC");
    }

    public CsvCell getProc10CodingType() {
        return super.getCell("PROC_10_CODING_TYPE");
    }

    public CsvCell getProc11() {
        return super.getCell("PROC_11");
    }

    public CsvCell getProc11Description() {
        return super.getCell("PROC_11_DESC");
    }

    public CsvCell getProc11CodingType() {
        return super.getCell("PROC_11_CODING_TYPE");
    }

    public CsvCell getProc12() {
        return super.getCell("PROC_12");
    }

    public CsvCell getProc12Description() {
        return super.getCell("PROC_12_DESC");
    }

    public CsvCell getProc12CodingType() {
        return super.getCell("PROC_12_CODING_TYPE");
    }

    public CsvCell getDataUpdateStatus() {

        return super.getCell("DataUpdateStatus");
    }


    protected String getFileTypeDescription() {
        return "bhrutEpisodes Entry file ";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
