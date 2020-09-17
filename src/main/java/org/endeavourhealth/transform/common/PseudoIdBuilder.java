package org.endeavourhealth.transform.common;

import OpenPseudonymiser.Crypto;
import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.IdentifierHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.subscriberTransform.PseudoIdDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.PseudoIdAudit;
import org.endeavourhealth.transform.subscriber.json.ConfigParameter;
import org.endeavourhealth.transform.subscriber.json.LinkDistributorConfig;
import org.hl7.fhir.instance.model.Patient;

import java.text.SimpleDateFormat;
import java.util.*;

public class PseudoIdBuilder {

    public static final String PATIENT_FIELD_DOB = "date_of_birth";
    public static final String PATIENT_FIELD_NHS_NUMBER = "nhs_number";

    private String subscriberConfigName;
    private String saltKeyName;
    private byte[] saltBytes;
    private TreeMap<String, String> treeMap;

    private PseudoIdBuilder(String subscriberConfigName, String saltKeyName, String saltBase64) {
        this(subscriberConfigName, saltKeyName, Base64.getDecoder().decode(saltBase64));
    }

    private PseudoIdBuilder(String subscriberConfigName, String saltKeyName, byte[] saltBytes) {
        if (Strings.isNullOrEmpty(subscriberConfigName)) {
            throw new RuntimeException("Null or empty subscriber config name");
        }
        if (Strings.isNullOrEmpty(saltKeyName)) {
            throw new RuntimeException("Null or empty salt key name");
        }
        if (saltBytes == null) {
            throw new RuntimeException("Null salt key bytes");
        }
        this.subscriberConfigName = subscriberConfigName;
        this.saltKeyName = saltKeyName;
        this.saltBytes = saltBytes;
    }

    private boolean addValue(String fieldName, String fieldValue) {

        if (fieldName == null) {
            throw new RuntimeException("Null field name");
        }

        if (Strings.isNullOrEmpty(fieldValue)) {
            return false;
        }

        if (treeMap == null) {
            treeMap = new TreeMap<>();
        }

        treeMap.put(fieldName, fieldValue);
        return true;
    }

    private void reset() {
        this.treeMap = null;
    }

    private String createPseudoId() throws Exception {
        if (treeMap == null
                || treeMap.isEmpty()) {
            return null;
        }

        Crypto crypto = new Crypto();
        crypto.SetEncryptedSalt(saltBytes);
        String pseudoId = crypto.GetDigest(this.treeMap);


        return pseudoId;
    }

    private TreeMap<String, String> getKeys() {
        return treeMap;
    }

    private boolean addPatientValue(Patient fhirPatient, String fieldName, String fieldLabel, String fieldFormat) {

        if (fhirPatient == null) {
            throw new RuntimeException("Null patient for pseudo ID generation");
        }
        if (Strings.isNullOrEmpty(fieldName)) {
            throw new RuntimeException("Null or empty fieldName for pseudo ID generation");
        }
        if (Strings.isNullOrEmpty(fieldLabel)) {
            throw new RuntimeException("Null or empty fieldLabel for pseudo ID generation");
        }

        if (fieldName.equals(PATIENT_FIELD_DOB)) {

            if (fhirPatient.hasBirthDate()) {
                Date d = fhirPatient.getBirthDate();
                return addValueDate(fieldLabel, d, fieldFormat);
            } else {
                return false;
            }

        } else if (fieldName.equals(PATIENT_FIELD_NHS_NUMBER)) {

            String nhsNumber = IdentifierHelper.findNhsNumber(fhirPatient); //this will be in nnnnnnnnnn format
            if (!Strings.isNullOrEmpty(nhsNumber)) {
                return addValueNhsNumber(fieldLabel, nhsNumber, fieldFormat);
            } else {
                return false;
            }

        } else {
            throw new RuntimeException("Unsupported field name [" + fieldName + "]");
        }
    }

    private boolean addValueDate(String fieldLabel, Date d, String fieldFormat) {
        if (d == null) {
            return false;
        }

        //if no explicit format provided, assume one
        if (Strings.isNullOrEmpty(fieldFormat)) {
            fieldFormat = "dd-MM-yyyy";
        }

        SimpleDateFormat sdf = new SimpleDateFormat(fieldFormat);
        String value = sdf.format(d);
        return addValue(fieldLabel, value);
    }


    private boolean addValueNhsNumber(String fieldLabel, String nhsNumber, String fieldFormat) {

        if (Strings.isNullOrEmpty(nhsNumber)) {
            return false;
        }

        //if no explicit format provided, assume one
        if (Strings.isNullOrEmpty(fieldFormat)) {
            fieldFormat = "nnnnnnnnnn";
        }

        StringBuilder sb = new StringBuilder();

        int pos = 0;
        char[] chars = nhsNumber.toCharArray();

        char[] formatChars = fieldFormat.toCharArray();
        for (int i=0; i<formatChars.length; i++) {
            char formatChar = formatChars[i];
            if (formatChar == 'n') {
                if (pos < chars.length) {
                    char c = chars[pos];
                    sb.append(c);
                    pos ++;
                }

            } else if (Character.isAlphabetic(formatChar)) {
                throw new RuntimeException("Unsupported character " + formatChar + " in NHS number format [" + fieldFormat + "]");

            } else {
                sb.append(formatChar);
            }
        }

        String value = sb.toString();
        return addValue(fieldLabel, value);
    }

    public static String generatePsuedoIdFromConfig(String subscriberConfigName, Patient fhirPatient, LinkDistributorConfig config) throws Exception {

        List<LinkDistributorConfig> l = new ArrayList<>();
        l.add(config);

        Map<LinkDistributorConfig, PseudoIdAudit> map = generatePsuedoIdsFromConfigs(fhirPatient, subscriberConfigName, l);
        PseudoIdAudit audit = map.get(config);
        if (audit == null) {
            return null;
        } else {
            return audit.getPseudoId();
        }
    }

    public static Map<LinkDistributorConfig, PseudoIdAudit> generatePsuedoIdsFromConfigs(Patient fhirPatient, String subscriberConfigName, List<LinkDistributorConfig> configs) throws Exception {

        Map<LinkDistributorConfig, PseudoIdAudit> ret = new HashMap<>();
        List<PseudoIdAudit> toAudit = new ArrayList<>();

        for (LinkDistributorConfig config: configs) {

            PseudoIdBuilder builder = new PseudoIdBuilder(subscriberConfigName, config.getSaltKeyName(), config.getSalt());
            boolean ok = true;

            List<ConfigParameter> parameters = config.getParameters();
            for (ConfigParameter param : parameters) {

                String fieldName = param.getFieldName();
                String fieldFormat = param.getFormat();
                String fieldLabel = param.getFieldLabel();

                boolean foundValue = builder.addPatientValue(fhirPatient, fieldName, fieldLabel, fieldFormat);

                //if this element is mandatory, then fail if our field is empty
                Boolean mandatory = param.getMandatory();
                if (mandatory != null
                        && mandatory.booleanValue()
                        && !foundValue) {
                    ok = false;
                    break;
                }
            }

            if (ok) {
                String pseudoId = builder.createPseudoId();
                if (!Strings.isNullOrEmpty(pseudoId)) {
                    PseudoIdAudit audit = new PseudoIdAudit(config.getSaltKeyName(), builder.getKeys(), pseudoId);
                    ret.put(config, audit);
                    toAudit.add(audit);
                }
            }
        }

        //make sure to always audit everything we've generated
        if (!toAudit.isEmpty()) {
            PseudoIdDalI pseudoIdDal = DalProvider.factoryPseudoIdDal(subscriberConfigName);
            pseudoIdDal.auditPseudoIds(toAudit);
        }

        return ret;
    }
}
