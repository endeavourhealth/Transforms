package org.endeavourhealth.transform.vision.helpers;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.core.database.dal.audit.models.TransformWarning;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisClinicalCode;
import org.endeavourhealth.core.terminology.Read2Code;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.endeavourhealth.transform.vision.schema.Journal;
import org.hl7.fhir.instance.model.CodeableConcept;
import org.hl7.fhir.instance.model.Coding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VisionCodeHelper {
    private static final Logger LOG = LoggerFactory.getLogger(VisionCodeHelper.class);



    public static void populateCodeableConcept(boolean isMedication, Journal parser, CodeableConceptBuilder codeableConceptBuilder, VisionCsvHelper csvHelper) throws Exception {

        //depending if adding medication or not, we get the snomed/DM+D concept ID from a different cell
        CsvCell snomedConceptCell = null;
        if (isMedication) {
            snomedConceptCell = parser.getDrugDMDCode();
        } else {
            snomedConceptCell = parser.getSnomedCode();
        }

        CsvCell readCodeCell = parser.getReadCode();
        CsvCell termCell = parser.getRubric();

        populateCodeableConcept(snomedConceptCell, readCodeCell, termCell, codeableConceptBuilder, csvHelper);
    }

    public static void populateCodeableConcept(CsvCell snomedConceptCell, CsvCell readCodeCell, CsvCell termCell,
                                               CodeableConceptBuilder codeableConceptBuilder, VisionCsvHelper csvHelper) throws Exception {

        //add the Snomed/DM+D column
        //null check because column isn't present in test pack
        Long snomedConcept = formatSnomedConcept(snomedConceptCell, csvHelper);
        if (snomedConcept != null) {
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            codeableConceptBuilder.setCodingCode(snomedConceptCell.getString(), snomedConceptCell);

            //look up the official term for this Snomed code
            Long snomedConceptId = snomedConceptCell.getLong();
            String snomedTerm = SnomedCache.lookUpSnomedTermForConcept(snomedConceptId);
            if (snomedTerm != null) {
                codeableConceptBuilder.setCodingDisplay(snomedTerm);
            }
        }

        //add the Read2 code

        String readCode = formatReadCode(readCodeCell, csvHelper);
        if (!Strings.isNullOrEmpty(readCode)) {

            //look up if this code is a proper Read2 code
            String term = Read2Cache.lookUpRead2TermForCode(readCode);
            if (term == null) {
                //if no term could be found, it's not an official Read2 code, so much be a Vision local code (or garbage)

                //if we have a Rubric, then we can treat it as a Vision local code, but if we don't, then it's just bad data
                if (termCell.isEmpty()) {
                    TransformWarnings.log(LOG, csvHelper, "Vision local code without Rubric {}", readCodeCell);
                    codeableConceptBuilder.setText(readCode, readCodeCell);
                    return;
                }

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_VISION_CODE);
                codeableConceptBuilder.setCodingCode(readCode, readCodeCell);
                codeableConceptBuilder.setCodingDisplay(termCell.getString(), termCell);

            } else {
                //if we found an official Read2 term, then it's an official Read2 code
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_READ2);
                codeableConceptBuilder.setCodingCode(readCode, readCodeCell);
                codeableConceptBuilder.setCodingDisplay(term);
            }
        }

        //set the term, using the Rubric if present, or one of the official terms otherwise
        if (!termCell.isEmpty()) {
            codeableConceptBuilder.setText(termCell.getString(), termCell);

        } else {
            //if not term provided, use the term from one of the codings
            //Snomed will always be first, so go from the start and break once we've found one
            CodeableConcept cc = codeableConceptBuilder.getCodeableConcept();
            for (Coding coding : cc.getCoding()) {
                if (coding.hasDisplay()) {
                    codeableConceptBuilder.setText(coding.getDisplay());
                    break;
                }
            }
        }
    }

    /**
     * there are a couple of "Snomed" concept IDs that are garbage and clearly nothing like a real Snomed concept
     * (including locally authored ones), so this checks for them, logs any bad ones and returns only proper-looking codes
     * Known bad values are:
     * 29993973518322896898
     * 39217345555177672708
     */
    public static Long formatSnomedConcept(CsvCell snomedConceptCell, VisionCsvHelper csvHelper) throws Exception {
        if (snomedConceptCell == null //test pack doesn't have the snomed columns
                || snomedConceptCell.isEmpty()) {
            return null;
        }

        try {
            return snomedConceptCell.getLong();

        } catch (NumberFormatException nfe) {
            TransformWarnings.log(LOG, csvHelper, "Bad Vision snomed concept ID {}", snomedConceptCell);
            return null;
        }
    }

    /**
     * the "code" column NORMALLY contains a five-character Read code followed by a two-digit term code, e.g. ABCDE00
     * however, there are a small, but non-zero number of cases where the column contains something obviously
     * not a Read code e.g. "DNA" or it looks like a Read code but only has a single term code.
     *
     *
     * https://endeavourhealth.atlassian.net/browse/SD-192 "9V0.00"
     * https://endeavourhealth.atlassian.net/browse/SD-190 "13m1.0"
     * https://endeavourhealth.atlassian.net/browse/SD-186 "DNA"
     */
    public static String formatReadCode(CsvCell readCodeCell, VisionCsvHelper csvHelper) throws Exception {

        if (readCodeCell.isEmpty()) {
            return null;
        }

        String code = readCodeCell.getString();

        if (code.length() <= 4) {
            //all codes under four chars are invalid Read2 codes and do not fit
            //with Vision local codes so just return them as they are
            TransformWarnings.log(LOG, csvHelper, "Non-valid Vision journal code {}", readCodeCell);
            return code;

        } else if (code.length() == 5) {
            //if length five, it may be a real Read2 or maybe not, but don't do anything to it
            return code;

        } else if (code.length() == 6) {
            //there are only a small number of six-character codes, and some a legit Read2 codes with an additional
            //single-digit term code, and others look like garbage
            String prefix = code.substring(0, 5);
            if (Read2Cache.isRealRead2Code(prefix)) {
                return prefix;

            } else {
                //there are only three six-character non-valid Read2 codes that have been found in the first 30 practices
                //so rather than attempt to come up with some algorithm to process them, just let those specific ones through
                //and throw an exception if it's something else
                if (code.equals("#9V0..")
                        || code.equals("9V0.00")
                        || code.equals("asthma")
                        || code.equals("9mb.00") //SD-358 - new code with empty RUBRIC field, so just treat like the above ones
                        ) {
                    return code;

                } else {
                    throw new Exception("Unexpected six-character Vision code [" + code + "]");
                }
            }

        } else if (code.length() == 7) {
            //the vast majority of codes are seven-characters, with a five-letter Read2 code (or local code) and
            //then a two-digit term code
            String prefix = code.substring(0, 5);
            if (Read2Cache.isRealRead2Code(prefix)) {
                //if the first five characters are a true Read2 code, then use that as the code
                return prefix;
            } else {
                //if not a true Read2 code then validate that the last two digits are a numeric term code
                String suffix = code.substring(5);
                try {
                    //if the suffix is a numeric term code, then this is just a Vision local code, so return the five-character prefix
                    Integer.parseInt(suffix);
                    return prefix;

                } catch (NumberFormatException nfe) {
                    //if the suffix isn't a numeric term code, then the format of the cell is off, and it's not something we
                    //should silently process
                    throw new Exception("Unexpected seven-character Vision code [" + code + "]");
                }
            }

        } else {
            //if any code is longer than seven chars, then this is new to us, so throw an exception so we can investigate
            throw new Exception("Unexpected " + code.length() + " character code [" + code.length() + "]");
        }
    }


}
