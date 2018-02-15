package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.AbstractCharacterParser;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

public class LOREF extends AbstractCharacterParser {
    private static final Logger LOG = LoggerFactory.getLogger(LOREF.class);

    public static final String DATE_FORMAT = "dd/mm/yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public LOREF(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, boolean openParser) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, "\\|", openParser, DATE_FORMAT, TIME_FORMAT);

        addFieldList("LocationHistoryKey");
        addFieldList("LocationId");
        addFieldList("ExtractDateTime");
        addFieldList("BeginEffectiveDateTime");
        addFieldList("EndEffectiveDateTime");
        addFieldList("BedLcoation");
        addFieldList("RoomLocation");
        addFieldList("NurseUnitLocation");
        addFieldList("AmbulatoryLocation");
        addFieldList("SurgeryLocation");
        addFieldList("BuildingLocation");
        addFieldList("FacilityLocation");

    }

    public String getLocationId() throws FileFormatException {
        return super.getString("LocationId");
    }

    public Date getExtractDateTime() throws TransformException {
        return super.getDateTime("ExtractDateTime");
    }

    public Date getBeginEffectiveDateTime() throws TransformException {
        return super.getDateTime("BeginEffectiveDateTime");
    }

    public Date getEndEffectiveDateTime() throws TransformException {
        return super.getDateTime("EndEffectiveDateTime");
    }

    public String getBedLcoation() throws FileFormatException {
        return super.getString("BedLcoation");}

    public String getRoomLocation() throws FileFormatException {
        return super.getString("RoomLocation");}

    public String getNurseUnitLocation() throws FileFormatException {
        return super.getString("NurseUnitLocation");}

    public String getAmbulatoryLocation() throws FileFormatException {
        return super.getString("AmbulatoryLocation");}

    public String getBuildingLocation() throws FileFormatException {
        return super.getString("BuildingLocation");}

    public String getFacilityLocation() throws FileFormatException {
        return super.getString("FacilityLocation");}

    @Override
    protected String getFileTypeDescription() {
        return "Cerner location file";
    }


}