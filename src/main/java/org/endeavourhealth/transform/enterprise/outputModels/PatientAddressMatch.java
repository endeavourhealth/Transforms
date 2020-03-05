package org.endeavourhealth.transform.enterprise.outputModels;

import org.apache.commons.csv.CSVFormat;
import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.util.Date;

public class PatientAddressMatch extends AbstractEnterpriseCsvWriter {

    private boolean pseduonymised = false;

    public PatientAddressMatch(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat, boolean pseduonymised) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);

        this.pseduonymised = pseduonymised;
    }

    public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }

    public boolean isPseduonymised() {
        return this.pseduonymised;
    }

    public void writeUpsertPseudonymised(long id,
                                         long personId,
                                         String uprn,
                                         Integer status,
                                         String classification,
                                         String qualifier,
                                         String algorithm,
                                         Date match_date,
                                         String algorithm_version,
                                         String epoc
    ) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id, // patient_id
                "" + personId,
                uprn,
                convertInt(status),
                classification,
                qualifier,
                algorithm,
                convertDateTime(match_date),
                algorithm_version,
                epoc
        );
    }

    public void writeUpsert(long id,
                            long personId,
                            long uprn,
                            Integer status,
                            String classification,
                            BigDecimal latitude,
                            BigDecimal longitude,
                            BigDecimal xcoordinate,
                            BigDecimal ycoordinate,
                            String qualifier,
                            String algorithm,
                            Date match_date,
                            String abp_address_number,
                            String abp_address_street,
                            String abp_address_locality,
                            String abp_address_town,
                            String abp_address_postcode,
                            String abp_address_organization,
                            String match_pattern_postcode,
                            String match_pattern_street,
                            String match_pattern_number,
                            String match_pattern_building,
                            String match_pattern_flat,
                            String algorithm_version,
                            String epoc
    ) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id, // patient_id
                "" + personId,
                convertLong(uprn),
                convertInt(status),
                classification,
                convertBigDecimal(latitude),
                convertBigDecimal(longitude),
                convertBigDecimal(xcoordinate),
                convertBigDecimal(ycoordinate),
                qualifier,
                algorithm,
                convertDateTime(match_date),
                abp_address_number,
                abp_address_street,
                abp_address_locality,
                abp_address_town,
                abp_address_postcode,
                abp_address_organization,
                match_pattern_postcode,
                match_pattern_street,
                match_pattern_number,
                match_pattern_building,
                match_pattern_flat,
                algorithm_version,
                epoc
        );
    }

    @Override
    public Class[] getColumnTypes() {
        if (isPseduonymised()) {
            return new Class[]{
                    String.class, // thingy
                    Long.TYPE, // id (bigint)
                    Long.TYPE, // person_id (bigint)
                    String.class, // uprn (string)
                    Integer.class, // status (tinyint)
                    String.class, // classification (varchar)
                    String.class, // qualifier (varchar)
                    String.class, // algorithm (varchar)
                    Date.class, // match_date (datetime)
                    String.class, // algorithm_version (varchar)
                    String.class // epoc (varchar)
            };
        } else {
            return new Class[]{
                    String.class, // thingy
                    Long.TYPE, // id (bigint)
                    Long.TYPE, // person_id (bigint)
                    Long.TYPE, // uprn (bigint)
                    Integer.class, // status (tinyint)
                    String.class, // classification (varchar)
                    BigDecimal.class, // latitude (double)
                    BigDecimal.class, // longitude (double)
                    BigDecimal.class, // x (double)
                    BigDecimal.class, // y (double)
                    String.class, // qualifier (varchar)
                    String.class, // algorithm (varchar)
                    Date.class, // match_date (datetime)
                    String.class, // abp_address_number (varchar)
                    String.class, // abp_address_street (varchar)
                    String.class, // abp_address_locality (varchar)
                    String.class, // abp_address_town (varchar)
                    String.class, // abp_address_postcode (varchar)
                    String.class, // abp_address_organization (varchar)
                    String.class, // match_pattern_postcode (varchar)
                    String.class, // match_pattern_street (varchar)
                    String.class, // match_pattern_number (varchar)
                    String.class, // match_pattern_building (varchar)
                    String.class, // match_pattern_flat (varchar)
                    String.class, // algorithm_version (varchar)
                    String.class // epoc (varchar)
            };
        }
    }

    @Override
    public String[] getCsvHeaders() {
        if (isPseduonymised()) {
            return new String[]{
                    "save_mode",
                    "patient_id",
                    "person_id",
                    "uprn",
                    "status",
                    "classification",
                    "qualifier",
                    "algorithm",
                    "match_date",
                    "algorithm_version",
                    "epoc"
            };
        } else {
            return new String[]{
                    "save_mode",
                    "patient_id",
                    "person_id",
                    "uprn",
                    "status",
                    "classification",
                    "latitude",
                    "longitude",
                    "xcoordinate",
                    "ycoordinate",
                    "qualifier",
                    "algorithm",
                    "match_date",
                    "abp_address_number",
                    "abp_address_street",
                    "abp_address_locality",
                    "abp_address_town",
                    "abp_address_postcode",
                    "abp_address_organization",
                    "match_pattern_postcode",
                    "match_pattern_street",
                    "match_pattern_number",
                    "match_pattern_building",
                    "match_pattern_flat",
                    "algorithm_version",
                    "epoc"
            };
        }
    }
}

                