package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.AbstractCharacterParser;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class DIAGN extends AbstractCharacterParser {
    private static final Logger LOG = LoggerFactory.getLogger(DIAGN.class);

    public static final String DATE_FORMAT = "dd/mm/yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public DIAGN(String version, String filePath, boolean openParser) throws Exception {
        super(version, filePath, "\\|", openParser, DATE_FORMAT, TIME_FORMAT);

        addFieldList("MillenniumDiagnosisId");
        addFieldList("ActiveIndicator");
        addFieldList("ExtractDateTime");
        addFieldList("MillenniumEncounterIdentifier");
        addFieldList("MillenniumEncounterSliceIdentifier");
        addFieldList("CDSSequence");
        addFieldList("MillenniumNomenclatureIdentifier");
        addFieldList("DiagnosisDateTime");
        addFieldList("DiagnosisMillenniumPersonnelIdentifier");
        addFieldList("DiagnosisTypeMillenniumCode");
        addFieldList("MillenniumConceptIdentifier");
        addFieldList("DiagnosisFreeText");
        addFieldList("DiagnosisCodeSequenceEntryOrder");
    }

    public String getDiagnosisID() throws FileFormatException {
        return super.getString("MillenniumDiagnosisId");
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

    public String getEncounterID() throws FileFormatException {
        return super.getString("MillenniumEncounterIdentifier");
    }

    public String getEncounterSliceID() throws FileFormatException {
        return super.getString("MillenniumEncounterSliceIdentifier");
    }

    public String getCDSSequence() throws FileFormatException {
        return super.getString("CDSSequence");
    }

    public String getNomenclatureID() throws FileFormatException {
        return super.getString("MillenniumNomenclatureIdentifier");
    }

    public Date getDiagnosisDateTime() throws TransformException {
        return super.getDate("DiagnosisDateTime");
    }

    public String getDiagnosisDateTimeAsString() throws TransformException {
        return super.getString("DiagnosisDateTime");
    }

    public String getPersonnel() throws FileFormatException {
        return super.getString("DiagnosisMillenniumPersonnelIdentifier");
    }

    public Long getDiagnosisTypeCode() throws FileFormatException {
        return super.getLong("DiagnosisTypeMillenniumCode");
    }

    public String getConceptCodeIdentifier() throws FileFormatException {
        return super.getString("MillenniumConceptIdentifier");
    }

    public String getConceptCodeType() throws FileFormatException {
        String conceptCodeIdentifier = super.getString("MillenniumConceptIdentifier");
        int index = conceptCodeIdentifier.indexOf('!');
        if (index > -1) {
            return conceptCodeIdentifier.substring(0,index);
        } else {
            return null;
        }
    }

    public String getConceptCode() throws FileFormatException {
        String conceptCodeIdentifier = super.getString("MillenniumConceptIdentifier");
        int index = conceptCodeIdentifier.indexOf('!');
        if (index > -1) {
            return conceptCodeIdentifier.substring(index + 1);
        } else {
            return null;
        }
    }

    public String getDiagnosicFreeText() throws FileFormatException {
        return super.getString("DiagnosisFreeText");
    }

    public String getProcedureCodeSequenceEntryOrder() throws FileFormatException {
        return super.getString("DiagnosisCodeSequenceEntryOrder");
    }
}