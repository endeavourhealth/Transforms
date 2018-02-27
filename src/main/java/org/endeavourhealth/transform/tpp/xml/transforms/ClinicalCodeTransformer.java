package org.endeavourhealth.transform.tpp.xml.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.FhirHelper;
import org.endeavourhealth.transform.tpp.xml.schema.*;
import org.hl7.fhir.instance.model.*;
import org.hl7.fhir.instance.model.Patient;

import javax.xml.datatype.XMLGregorianCalendar;
import java.math.BigDecimal;
import java.util.List;

public class ClinicalCodeTransformer {

    public static void transform(List<ClinicalCode> tppClinicalCodes, Event tppEvent, Encounter fhirEncounter, List<Resource> fhirResources) throws TransformException {
        for (ClinicalCode tppCode: tppClinicalCodes) {
            transform(tppCode, tppEvent, fhirEncounter, fhirResources);
        }
    }

    public static void transform(ClinicalCode tppCode, Event tppEvent, Encounter fhirEncounter, List<Resource> fhirResources) throws TransformException {

        if (tppCode.getIsAllergy() != null) {
            createAllergy(tppCode, tppEvent, fhirEncounter, fhirResources);
        } else if (tppCode.getIsFamilyHistory() != null) {
            createFamilyHistory(tppCode, tppEvent, fhirEncounter, fhirResources);
        }

        //TODO - need to work out difference between Conditions, Observations and Procedures

        //need to create:
        //AllergyIntolerance
        //Conditions
        //FamilyMemberHistory <- doesn't the API have an indicator for this???
        //Observation
        //Problem
        //Procedure


 /*       Code code = tppCode.getCode();
        Episodicity episodicity = tppCode.getEpisodicity();
        BigDecimal value = tppCode.getValue();
        String units = tppCode.getUnits();
        ProblemSeverity severity = tppCode.getProblemSeverity();
        XMLGregorianCalendar endDate = tppCode.getProblemEndDate();
        String freeText = tppCode.getFreeText();
        List<String> linkedProblemUIDs = tppCode.getLinkedProblemUID();
*/

        if (tppCode.getProblemSeverity() != null) {
            createProblem(tppCode, tppEvent, fhirEncounter, fhirResources);
        }
    }

    private static void createProblem(ClinicalCode tppCode, Event tppEvent, Encounter fhirEncounter, List<Resource> fhirResources) throws TransformException {

        Condition fhirProblem = new Condition();
        fhirProblem.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_PROBLEM));
        fhirResources.add(fhirProblem);

        fhirProblem.setPatient(FhirHelper.findAndCreateReference(Patient.class, fhirResources));

        if (fhirEncounter != null) {
            String encounterId = fhirEncounter.getId();
            fhirProblem.setEncounter(ReferenceHelper.createReference(ResourceType.Encounter, encounterId));
        }

        Code code = tppCode.getCode();
        //Episodicity episodicity = tppCode.getEpisodicity(); //not required for Problems
        //BigDecimal value = tppCode.getValue(); //not required for Problems
        //String units = tppCode.getUnits(); //not required for Problems
        ProblemSeverity severity = tppCode.getProblemSeverity();
        XMLGregorianCalendar endDate = tppCode.getProblemEndDate();
        String freeText = tppCode.getFreeText();
        List<String> linkedProblemUIDs = tppCode.getLinkedProblemUID();

        String userName = tppEvent.getUserName();
        if (!Strings.isNullOrEmpty(userName)) {
            fhirProblem.setAsserter(ReferenceHelper.createReference(ResourceType.Practitioner, userName));
        }

        fhirProblem.setCode(CodeTransformer.transform(code));

        //TODO - can't work out correct ClinicalStatus value from data
        //fhirProblem.setClinicalStatus()

        //data provided should always be confirmed - that's the way it's treated in the source system
        fhirProblem.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED);



        //severity
        //onset date
        //abatement date
        //notes
        //significance
    }



    private static void createFamilyHistory(ClinicalCode tppCode, Event tppEvent, Encounter fhirEncounter, List<Resource> fhirResources) {

        Code code = tppCode.getCode();
        Episodicity episodicity = tppCode.getEpisodicity();
        BigDecimal value = tppCode.getValue();
        String units = tppCode.getUnits();
        ProblemSeverity severity = tppCode.getProblemSeverity();
        XMLGregorianCalendar endDate = tppCode.getProblemEndDate();
        String freeText = tppCode.getFreeText();
        List<String> linkedProblemUIDs = tppCode.getLinkedProblemUID();

    }

    private static void createAllergy(ClinicalCode tppCode, Event tppEvent, Encounter fhirEncounter, List<Resource> fhirResources) {

        Code code = tppCode.getCode();
        Episodicity episodicity = tppCode.getEpisodicity();
        BigDecimal value = tppCode.getValue();
        String units = tppCode.getUnits();
        ProblemSeverity severity = tppCode.getProblemSeverity();
        XMLGregorianCalendar endDate = tppCode.getProblemEndDate();
        String freeText = tppCode.getFreeText();
        List<String> linkedProblemUIDs = tppCode.getLinkedProblemUID();

    }
}
