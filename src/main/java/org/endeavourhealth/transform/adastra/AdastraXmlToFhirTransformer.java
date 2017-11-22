package org.endeavourhealth.transform.adastra;

import org.endeavourhealth.common.utility.XmlHelper;
import org.endeavourhealth.transform.adastra.schema.AdastraCaseDataExport;
import org.endeavourhealth.transform.adastra.schema.CodedItem;
import org.endeavourhealth.transform.adastra.transforms.admin.LocationTransform;
import org.endeavourhealth.transform.adastra.transforms.admin.OrganisationTransformer;
import org.endeavourhealth.transform.adastra.transforms.admin.PatientTransformer;
import org.endeavourhealth.transform.adastra.transforms.admin.UserTransform;
import org.endeavourhealth.transform.adastra.transforms.clinical.EncounterTransform;
import org.endeavourhealth.transform.adastra.transforms.clinical.EpisodeTransformer;
import org.endeavourhealth.transform.adastra.transforms.clinical.FlagTransform;
import org.endeavourhealth.transform.adastra.transforms.clinical.ObservationTransformer;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.hl7.fhir.instance.model.Resource;

import java.util.ArrayList;
import java.util.List;

import static org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper.uniqueIdMapper;

public class AdastraXmlToFhirTransformer {

    public static List<Resource> toFhirFullRecord(String xmlPayload) throws Exception {

        AdastraCaseDataExport caseReport = XmlHelper.deserialize(xmlPayload, AdastraCaseDataExport.class);

        checkMessageForIssues(caseReport);

        List<Resource> ret = new ArrayList<>();

        OrganisationTransformer.transform(caseReport, ret);
        LocationTransform.transform(caseReport, ret);
        UserTransform.transform(caseReport, ret);
        PatientTransformer.transform(caseReport, ret);
        EpisodeTransformer.transform(caseReport, ret);
        EncounterTransform.createMainCaseEncounter(caseReport, ret);
        String mainEncounterId = uniqueIdMapper.get("caseEncounter");

        if (caseReport.getConsultation() != null) {
            EncounterTransform.createChildEncountersFromConsultations(caseReport, ret);
        }

        if (caseReport.getOutcome() != null) {
            for (CodedItem codedItem : caseReport.getOutcome()) {
                ObservationTransformer.observationFromCodedItem(codedItem, mainEncounterId,
                        caseReport.getActiveDate(), caseReport.getAdastraCaseReference(),
                        "Outcome", ret);
            }
        }

        if (caseReport.getPresentingCondition() != null)
            ObservationTransformer.observationFromPresentingCondition(caseReport, ret);

        if  (caseReport.getQuestions() != null && !caseReport.getQuestions().isEmpty()) {
            ObservationTransformer.observationFromFreeText(caseReport.getQuestions(), mainEncounterId,
                    caseReport.getActiveDate(), caseReport.getAdastraCaseReference(),
                    "Questions", ret);
        }

        if (caseReport.getSpecialNote() != null) {
            for (AdastraCaseDataExport.SpecialNote specialNote : caseReport.getSpecialNote()) {
                FlagTransform.transform(specialNote, caseReport.getAdastraCaseReference(), ret);
            }
        }

        return ret;
    }


    private static void checkMessageForIssues(AdastraCaseDataExport caseReport) throws TransformException {
        if (caseReport.getAdastraCaseReference() == null) {
            throw new TransformException("Case Reference not specified in message");
        }

        if (caseReport.getPatient() == null) {
            throw new TransformException("Patient not specified in message");
        }

        if (caseReport.getPatient().getGpRegistration() == null) {
            throw new TransformException("GP Registration not specified in message");
        }
    }
}
