package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.AbstractCharacterParser;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class PRSNLREF extends AbstractCharacterParser {
    private static final Logger LOG = LoggerFactory.getLogger(PRSNLREF.class);

    public static final String DATE_FORMAT = "dd/mm/yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public PRSNLREF(String version, String filePath, boolean openParser) throws Exception {
        super(version, filePath, "\\|", openParser, DATE_FORMAT, TIME_FORMAT);

        addFieldList("MillenniumPersonnelId");
        addFieldList("ExtractDateTime");
        addFieldList("ActiveIndicator");
        addFieldList("MillenniumPositionCode");
        addFieldList("PhysicianIndicator");
        addFieldList("Title");
        addFieldList("FirstName");
        addFieldList("MiddleName");
        addFieldList("LastName");
        addFieldList("FullFormatName");
        addFieldList("Address1");
        addFieldList("Address2");
        addFieldList("Address3");
        addFieldList("Address4");
        addFieldList("PostCode");
        addFieldList("City");
        addFieldList("MillenniumCountryCode");
        addFieldList("MillenniumCountyCode");
        addFieldList("Email");
        addFieldList("Fax");
        addFieldList("Phone");
        addFieldList("ConsultantNHSCode");
        addFieldList("MillenniumSpecialtyCode");
        addFieldList("GPNHSCode");   //gmp code
    }

    public String getPersonnelID() throws FileFormatException {
        return super.getString("MillenniumPersonnelId");
    }

    public int getActiveIndicator() throws FileFormatException {
        return super.getInt("ActiveIndicator");
    }

    public Date getExtractDateTime() throws TransformException {
        return super.getDateTime("ExtractDateTime");
    }

    public boolean isActive() throws FileFormatException {
        int val = super.getInt("ActiveIndicator");
        if (val == 1) {
            return true;
        } else {
            return false;
        }
    }

    public Long getMilleniumPositionCode() throws FileFormatException {
        return super.getLong("MillenniumPositionCode");
    }

    public String getPhysicianIndicator() throws FileFormatException {
        return super.getString("PhysicianIndicator");
    }

    public String getTitle() throws FileFormatException {
        return super.getString("Title");
    }

    public String getFirstName() throws FileFormatException {
        return super.getString("FirstName");
    }

    public String getMiddleName() throws TransformException {
        return super.getString("MiddleName");
    }

    public String getLastName() throws TransformException {
        return super.getString("LastName");
    }

    public String getFullFormatName() throws FileFormatException {
        return super.getString("FullFormatName");
    }

    public String getAddress1() throws FileFormatException {
        return super.getString("Address1");
    }

    public String getAddress2() throws FileFormatException {
        return super.getString("Address2");
    }

    public String getAddress3() throws FileFormatException {
        return super.getString("Address3");
    }

    public String getAddress4() throws FileFormatException {
        return super.getString("Address4");
    }

    public String getPostCode() throws FileFormatException {
        return super.getString("PostCode");
    }

    public String getCity() throws FileFormatException {
        return super.getString("City");
    }

    public String getMillenniumCountryCode() throws FileFormatException {
        return super.getString("MillenniumCountryCode");
    }

    public String getEmail() throws FileFormatException {
        return super.getString("Email");
    }

    public String getFax() throws FileFormatException {
        return super.getString("Fax");
    }

    public String getPhone() throws FileFormatException {
        return super.getString("Phone");
    }

    public String getConsultantNHSCode() throws FileFormatException {
        return super.getString("ConsultantNHSCode");
    }

    public Long getMillenniumSpecialtyCode() throws FileFormatException {
        return super.getLong("MillenniumSpecialtyCode");
    }

    public String getGPNHSCode() throws FileFormatException {
        return super.getString("GPNHSCode");
    }
}