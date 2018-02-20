package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.AbstractCharacterParser;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

public class LOREF extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(LOREF.class);

    //public static final String DATE_FORMAT = "dd/mm/yyyy";
    //public static final String TIME_FORMAT = "hh:mm:ss";
    //public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public LOREF(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, boolean openParser) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, openParser,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                BartsCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "#LOCATION_HISTORY_KEY",
                "LOCATION_ID",
                "EXTRACT_DT_TM",
                "BEG_EFFECTIVE_DT_TM",
                "END_EFFECTIVE_DT_TM",
                "BED_LOC_CD",
                "ROOM_LOC_CD",
                "NURSE_UNIT_LOC_CD",
                "AMBULATORY_LOC_CD",
                "SURGERY_LOC_CD",
                "BUILDING_LOC_CD",
                "FACILITY_LOC_CD"
        };
    }

    public CsvCell getLocationId() throws FileFormatException {
        return super.getCell("LOCATION_ID");
    }

    public CsvCell getExtractDateTime() throws TransformException {
        return super.getCell("EXTRACT_DT_TM");
    }

    public CsvCell getBeginEffectiveDateTime() throws TransformException {
        return super.getCell("BEG_EFFECTIVE_DT_TM");
    }

    public CsvCell getEndEffectiveDateTime() throws TransformException {
        return super.getCell("END_EFFECTIVE_DT_TM");
    }

    public CsvCell getBedLcoation() throws FileFormatException {
        return super.getCell("BED_LOC_CD");}

    public CsvCell getRoomLocation() throws FileFormatException {
        return super.getCell("ROOM_LOC_CD");}

    public CsvCell getNurseUnitLocation() throws FileFormatException {
        return super.getCell("NURSE_UNIT_LOC_CD");}

    public CsvCell getAmbulatoryLocation() throws FileFormatException {
        return super.getCell("AMBULATORY_LOC_CD");}

    public CsvCell getSurgeryLocation() throws FileFormatException {
        return super.getCell("SURGERY_LOC_CD");}

    public CsvCell getBuildingLocation() throws FileFormatException {
        return super.getCell("BUILDING_LOC_CD");}

    public CsvCell getFacilityLocation() throws FileFormatException {
        return super.getCell("FACILITY_LOC_CD");}

    @Override
    protected String getFileTypeDescription() {
        return "Cerner location file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }


}