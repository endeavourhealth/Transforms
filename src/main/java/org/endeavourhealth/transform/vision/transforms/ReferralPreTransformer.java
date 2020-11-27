package org.endeavourhealth.transform.vision.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.endeavourhealth.transform.vision.schema.Referral;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

public class ReferralPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ReferralPreTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 VisionCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Referral.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    processRecord((Referral) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void processRecord(Referral parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       VisionCsvHelper csvHelper) throws Exception {


        //cache linked problem, so it can be picked up later
        CsvCell linksCell = parser.getLinks();
        CsvCell patientIdCell = parser.getPatientID();
        CsvCell journalIdCell = parser.getReferralID();

        String consultationId = JournalTransformer.extractEncounterLinkId(linksCell);
        if (!Strings.isNullOrEmpty(consultationId)) {

            csvHelper.cacheNewConsultationChildRelationship(consultationId, patientIdCell, journalIdCell, ResourceType.ReferralRequest, linksCell);
        }

        Set<String> problemLinkIds = JournalTransformer.extractJournalLinkIds(linksCell);
        if (problemLinkIds != null) {

            for (String problemId: problemLinkIds) {

                //store the problem/observation relationship in the helper
                csvHelper.cacheProblemRelationship(problemId, patientIdCell, journalIdCell, ResourceType.ReferralRequest, linksCell);
            }
        }


    }
}
