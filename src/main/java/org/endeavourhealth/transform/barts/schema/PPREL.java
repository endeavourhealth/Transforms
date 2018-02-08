package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.AbstractCharacterParser;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class PPREL extends AbstractCharacterParser {

    private static final Logger LOG = LoggerFactory.getLogger(PPATI.class);

    public static final String DATE_FORMAT = "dd/mm/yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public PPREL(String version, String filePath, boolean openParser) throws Exception {
        super(version, filePath, "\\|", openParser, DATE_FORMAT, TIME_FORMAT);

        addFieldList("MillenniumPersonRelationId");
        addFieldList("ExtractDateTime");
        addFieldList("ActiveIndicator");
        addFieldList("MillenniumPersonIdentifier");
        addFieldList("RelatedPersonMillenniumIdentifier");
        addFieldList("RelationshipToPatientCode");
        addFieldList("PersonRelationTypeCode");
        addFieldList("Title");
        addFieldList("FirstName");
        addFieldList("MiddleName");
        addFieldList("LastName");
        addFieldList("AddressLine1");
        addFieldList("AddressLine2");
        addFieldList("AddressLine3");
        addFieldList("AddressLine4");
        addFieldList("City");
        addFieldList("Country");
        addFieldList("Postcode");
        addFieldList("HomePhoneNumber");
        addFieldList("MobilePhoneNumber");
        addFieldList("WorkPhoneNumber");
        addFieldList("EmailAddress");
        addFieldList("BeginEffectiveDateTime");
        addFieldList("EndEffectiveDateTime");

    }

    public String getMillenniumPersonRelationId() throws FileFormatException {
        return super.getString("MillenniumPersonRelationId");
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

    public String getRelatedPersonMillenniumIdentifier() throws FileFormatException {
        return super.getString("RelatedPersonMillenniumIdentifier");
    }

    public String  getRelationshipToPatientCode() throws FileFormatException {
        return super.getString("RelationshipToPatientCode");
    }

    public String getPersonRelationTypeCode() throws FileFormatException {
        return super.getString("PersonRelationTypeCode");
    }

    public String getTitle() throws FileFormatException {
        return super.getString("Title");
    }

    public String getFirstName() throws FileFormatException {
        return super.getString("FirstName");
    }

    public String getMiddleName() throws FileFormatException {
        return super.getString("MiddleName");
    }

    public String getLastName() throws FileFormatException {
        return super.getString("LastName");
    }

    public String getAddressLine1() throws TransformException {
        return super.getString("AddressLine1");
    }

    public String getAddressLine2() throws TransformException {
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

    public String getCountry() throws TransformException {
        return super.getString("Country");
    }

    public String getPostcode() throws TransformException {
        return super.getString("Postcode");
    }

    public String getHomePhoneNumber() throws FileFormatException {
        return super.getString("HomePhoneNumber");
    }

    public String getMobilePhoneNumber() throws FileFormatException {
        return super.getString("MobilePhoneNumber");
    }

    public String getWorkPhoneNumber() throws FileFormatException {
        return super.getString("WorkPhoneNumber");
    }

    public String getEmailAddress() throws TransformException {
        return super.getString("EmailAddress");
    }

    public String getBeginEffectiveDateTime() throws TransformException {
        return super.getString("BeginEffectiveDateTime");
    }

    public String getEndEffectiveDateTime() throws FileFormatException {
        return super.getString("EndEffectiveDateTime");
    }
}