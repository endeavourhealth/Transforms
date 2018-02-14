package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.AbstractCharacterParser;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

public class PPADD extends AbstractCharacterParser {

    private static final Logger LOG = LoggerFactory.getLogger(PPATI.class);

    public static final String DATE_FORMAT = "dd/mm/yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public PPADD(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, boolean openParser) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, "\\|", openParser, DATE_FORMAT, TIME_FORMAT);

        addFieldList("MillenniumAddressId");
        addFieldList("ExtractDateTime");
        addFieldList("ActiveIndicator");
        addFieldList("MillenniumPersonIdentifier");
        addFieldList("BeginEffectiveDate");
        addFieldList("EndEffectiveDate");
        addFieldList("AddressLine1");
        addFieldList("AddressLine2");
        addFieldList("AddressLine3");
        addFieldList("AddressLine4");
        addFieldList("City");
        addFieldList("Postcode");
        addFieldList("CountyCode");
        addFieldList("CountyText");
        addFieldList("CountryCode");
        addFieldList("CountryText");
        addFieldList("AddressTypeCode");
        addFieldList("AddressTypeSequence");
        addFieldList("ResidencePCTCodeValue");

    }

    public String getMillenniumAddressId() throws FileFormatException {
        return super.getString("MillenniumAddressId");
    }

    public Date getExtractDateTime() throws TransformException {
        return super.getDateTime("ExtractDateTime");
    }

    public String getActiveIndicator() throws FileFormatException {
        return super.getString("ActiveIndicator");
    }

    public boolean isActive() throws FileFormatException {
        int val = super.getInt("ActiveIndicator");
        if (val == 1) {
            return true;
        } else {
            return false;
        }
    }

    public String getMillenniumPersonIdentifier() throws FileFormatException {
        return super.getString("MillenniumPersonIdentifier");
    }

    public Date getBeginEffectiveDate() throws TransformException {
        return super.getDateTime("BeginEffectiveDate");
    }

    public Date getEndEffectiveDater() throws TransformException {
        return super.getDateTime("EndEffectiveDate");
    }

    public String getAddressLine1() throws FileFormatException {
        return super.getString("AddressLine1");
    }

    public String getAddressLine2() throws FileFormatException {
        return super.getString("AddressLine2");
    }

    public String getAddressLine3() throws FileFormatException {
        return super.getString("AddressLine3");
    }

    public String getAddressLine4() throws FileFormatException {
        return super.getString("AddressLine4");
    }

    public String getCity() throws FileFormatException {
        return super.getString("City");
    }

    public String getPostcode() throws TransformException {
        return super.getString("Postcode");
    }

    public String getCountyCode() throws TransformException {
        return super.getString("CountyCode");
    }

    public String getCountyText() throws FileFormatException {
        return super.getString("CountyText");
    }

    public String getCountryCode() throws FileFormatException {
        return super.getString("CountryCode");
    }

    public String getCountryText() throws FileFormatException {
        return super.getString("CountryText");
    }

    public String getAddressTypeCode() throws TransformException {
        return super.getString("AddressTypeCode");
    }

    public String getAddressTypeSequence() throws TransformException {
        return super.getString("AddressTypeSequence");
    }

    public String getResidencePCTCodeValue() throws FileFormatException {
        return super.getString("ResidencePCTCodeValue");
    }

    @Override
    protected String getFileTypeDescription() {
        return "Cerner address file";
    }
}