package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.v23.datatype.CX;
import ca.uhn.hl7v2.model.v23.datatype.TS;
import ca.uhn.hl7v2.model.v23.datatype.XCN;
import ca.uhn.hl7v2.model.v23.segment.PV1;
import ca.uhn.hl7v2.model.v23.segment.EVN;
import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.usermanager.caching.OrganisationCache;
import org.endeavourhealth.core.database.rdbms.datasharingmanager.models.OrganisationEntity;
import org.endeavourhealth.im.models.mapping.MapResponse;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedParametersBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.hl7v2fhir.helpers.AcuteHL7Helper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class EncounterTransformer {


    private static final Logger LOG = LoggerFactory.getLogger(EncounterTransformer.class);

    /**
     *
     * @param pv1
     * @param fhirResourceFiler
     * @param acuteHL7Helper
     * @param msgType
     * @param patientGuid
     * @throws Exception
     */
    public static void transformPV1ToEncounter(PV1 pv1, EVN evn, CX patientAccountNumber, FhirResourceFiler fhirResourceFiler, AcuteHL7Helper acuteHL7Helper, String msgType, String patientGuid, String odsCode, String msgId) throws Exception {
        CX visitNum = pv1.getVisitNumber();
        if(visitNum.getID().getValue() != null) {
            EncounterBuilder encounterBuilder = null;
            Encounter existingEncounter = null;
            String visitId = String.valueOf(visitNum.getID());
            existingEncounter = (Encounter) acuteHL7Helper.retrieveResource(visitId, ResourceType.Encounter);
            if (existingEncounter != null) {
                encounterBuilder = new EncounterBuilder(existingEncounter);
                encounterBuilder = updateExistingEncounterParent(pv1, acuteHL7Helper, msgType, patientGuid, encounterBuilder,odsCode);
                createChildEncounters(pv1,evn, patientAccountNumber, encounterBuilder, fhirResourceFiler, acuteHL7Helper, msgType, patientGuid, odsCode,msgId);

            } else {
                encounterBuilder = new EncounterBuilder();
                encounterBuilder = createEncountersParentMinimum(pv1, acuteHL7Helper, msgType, patientGuid, encounterBuilder,odsCode);
                createChildEncounters(pv1,evn, patientAccountNumber, encounterBuilder, fhirResourceFiler, acuteHL7Helper, msgType, patientGuid, odsCode,msgId);
            }
        }
    }

    /**
     *
     * @param pv1
     * @param acuteHL7Helper
     * @param msgType
     * @param patientGuid
     * @param parentTopEncounterBuilder
     * @return
     * @throws Exception
     */
    private static EncounterBuilder createEncountersParentMinimum(PV1 pv1, AcuteHL7Helper acuteHL7Helper, String msgType, String patientGuid, EncounterBuilder parentTopEncounterBuilder, String odsCode) throws Exception {
        Encounter.EncounterClass classAttr = null;
        if("E".equalsIgnoreCase(String.valueOf(pv1.getPatientClass()))) {
            classAttr = Encounter.EncounterClass.EMERGENCY;
            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(parentTopEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilder.setText("Emergency");

        } else if("I".equalsIgnoreCase(String.valueOf(pv1.getPatientClass()))) {
            classAttr = Encounter.EncounterClass.INPATIENT;
            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(parentTopEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilder.setText("Inpatient");

        } else if("O".equalsIgnoreCase(String.valueOf(pv1.getPatientClass()))) {
            classAttr = Encounter.EncounterClass.OUTPATIENT;
            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(parentTopEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilder.setText("Outpatient");
        }
        parentTopEncounterBuilder.setClass(classAttr);
        parentTopEncounterBuilder.setId(String.valueOf(pv1.getVisitNumber().getID()));

        TS admitDtTime = pv1.getAdmitDateTime();
        TS dischargeDtTime = pv1.getDischargeDateTime();
        Date dsDt=null;
        if ( !admitDtTime.isEmpty() && admitDtTime !=null) {
            String startDt = String.valueOf(admitDtTime.getTimeOfAnEvent());
            DateFormat admitFormat = new SimpleDateFormat("yyyyMMddHHmmss");
            admitFormat.setTimeZone(TimeZone.getTimeZone("Europe/London"));
            Date stDt = admitFormat.parse(startDt);
            parentTopEncounterBuilder.setPeriodStart(stDt);

            if (!dischargeDtTime.isEmpty()) {
                String endDt = String.valueOf(dischargeDtTime.getTimeOfAnEvent());
                DateFormat dischargeFormat = new SimpleDateFormat("yyyyMMddHHmmss");
                dischargeFormat.setTimeZone(TimeZone.getTimeZone("Europe/London"));
                dsDt = dischargeFormat.parse(endDt);
            }
        }

        if (("A03".equalsIgnoreCase(msgType) && (!dischargeDtTime.isEmpty()))) {
            parentTopEncounterBuilder.setPeriodEnd(dsDt);
            parentTopEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        } else {
            parentTopEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);
        }

        setCommonEncounterAttributes(pv1, parentTopEncounterBuilder, acuteHL7Helper, patientGuid, false, odsCode);
        return parentTopEncounterBuilder;
    }


    public static void deleteEndDate(Encounter parentEncounterBuilder, FhirResourceFiler fhirResourceFiler) throws Exception{
        EncounterBuilder encounterBuilder = new EncounterBuilder(parentEncounterBuilder);
        encounterBuilder.getPeriod().setEnd(null);
        fhirResourceFiler.savePatientResource(null, false, encounterBuilder);
    }


    /**
     *
     * @param pv1
     * @param acuteHL7Helper
     * @param msgType
     * @param patientGuid
     * @param parentTopEncounterBuilder
     * @return
     * @throws Exception
     */
    private static EncounterBuilder updateExistingEncounterParent(PV1 pv1, AcuteHL7Helper acuteHL7Helper, String msgType, String patientGuid, EncounterBuilder parentTopEncounterBuilder, String odsCode) throws Exception {
        Encounter.EncounterClass classAttr = null;
        if("E".equalsIgnoreCase(String.valueOf(pv1.getPatientClass()))) {
            classAttr = Encounter.EncounterClass.EMERGENCY;
            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(parentTopEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilder.setText("Emergency");

        } else if("I".equalsIgnoreCase(String.valueOf(pv1.getPatientClass()))) {
            classAttr = Encounter.EncounterClass.INPATIENT;
            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(parentTopEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilder.setText("Inpatient");

        } else if("O".equalsIgnoreCase(String.valueOf(pv1.getPatientClass()))) {
            classAttr = Encounter.EncounterClass.OUTPATIENT;
            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(parentTopEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilder.setText("Outpatient");
        }
        parentTopEncounterBuilder.setClass(classAttr);

        TS admitDtTime = pv1.getAdmitDateTime();
        TS dischargeDtTime = pv1.getDischargeDateTime();
        Date dsDt=null;
        if ( !admitDtTime.isEmpty() && admitDtTime !=null) {
            String startDt = String.valueOf(admitDtTime.getTimeOfAnEvent());
            DateFormat admitFormat = new SimpleDateFormat("yyyyMMddHHmmss");
            admitFormat.setTimeZone(TimeZone.getTimeZone("Europe/London"));
            Date stDt = admitFormat.parse(startDt);
            parentTopEncounterBuilder.setPeriodStart(stDt);

            if (!dischargeDtTime.isEmpty()) {
                String endDt = String.valueOf(dischargeDtTime.getTimeOfAnEvent());
                DateFormat dischargeFormat = new SimpleDateFormat("yyyyMMddHHmmss");
                dischargeFormat.setTimeZone(TimeZone.getTimeZone("Europe/London"));
                dsDt = dischargeFormat.parse(endDt);
            }
        }

        if (("A03".equalsIgnoreCase(msgType) && (!dischargeDtTime.isEmpty()))) {
            parentTopEncounterBuilder.setPeriodEnd(dsDt);
            parentTopEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        } else {
            parentTopEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);
        }
        setCommonEncounterAttributes(pv1, parentTopEncounterBuilder, acuteHL7Helper, patientGuid, false,odsCode);

        return parentTopEncounterBuilder;
    }

    /**
     *
     * @param pv1
     * @param builder
     * @param acuteHL7Helper
     * @throws Exception
     */
    private static void setCommonEncounterAttributes(PV1 pv1, EncounterBuilder builder, AcuteHL7Helper acuteHL7Helper, String patientGuid, boolean encounterInd, String odsCode) throws Exception {

        String patientId = String.valueOf(patientGuid);
        String patientVisitId = String.valueOf(pv1.getVisitNumber().getID());
        XCN[] consultingDoctor = pv1.getConsultingDoctor();
        String consultingDoctorId = null;
        if(consultingDoctor != null && consultingDoctor.length > 0) {
            consultingDoctorId = String.valueOf(consultingDoctor[0].getIDNumber());
        }

        XCN[] referringDoctor = pv1.getReferringDoctor();
        String referringDoctorId = null;
        if(referringDoctor != null && referringDoctor.length > 0) {
            referringDoctorId = String.valueOf(referringDoctor[0].getIDNumber());
        }

        if (null!=patientId && !patientId.isEmpty()) {
            Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, patientId);
            if (builder.isIdMapped()) {
                patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, acuteHL7Helper);
            }
            builder.setPatient(patientReference);
        }

        if (null!=patientVisitId && !patientVisitId.isEmpty()) {
            Reference episodeReference
                    = ReferenceHelper.createReference(ResourceType.EpisodeOfCare, patientVisitId);
            if (builder.isIdMapped()) {
                episodeReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(episodeReference, acuteHL7Helper);
            }
            builder.setEpisodeOfCare(episodeReference);
        }

        String loc = String.valueOf(pv1.getAssignedPatientLocation().getPointOfCare());
        if (null!=loc) {
            Reference patientAssignedLocReference
                    = ReferenceHelper.createReference(ResourceType.Location, loc);
            if (builder.isIdMapped()) {
                patientAssignedLocReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientAssignedLocReference, acuteHL7Helper);
            }

            if(builder.getLocation().size() > 0) {
                for (int i = 0; i < builder.getLocation().size(); i++) {
                    builder.getLocation().remove(i);
                }
            }
            builder.addLocation(patientAssignedLocReference);
            builder.getLocation().get(builder.getLocation().size() - 1).setStatus(Encounter.EncounterLocationStatus.ACTIVE);


        }

        if (null!=consultingDoctorId && (!consultingDoctorId.isEmpty())) {
            Reference practitionerReference
                    = ReferenceHelper.createReference(ResourceType.Practitioner, consultingDoctorId);
            if (builder.isIdMapped()) {

                practitionerReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, acuteHL7Helper);
            }
            builder.addParticipant(practitionerReference, EncounterParticipantType.PRIMARY_PERFORMER);
        }

        if (null!=referringDoctorId && (!referringDoctorId.isEmpty())) {
            Reference practitionerReferenceRd
                    = ReferenceHelper.createReference(ResourceType.Practitioner, referringDoctorId);
            if (builder.isIdMapped()) {

                practitionerReferenceRd
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReferenceRd, acuteHL7Helper);
            }
            builder.addParticipant(practitionerReferenceRd, EncounterParticipantType.SECONDARY_PERFORMER);
        }

        OrganisationEntity org = OrganisationCache.getOrganisationDetailsFromOdsCode(odsCode);
        String name = org.getName();

        Reference organizationReference
                = ReferenceHelper.createReference(ResourceType.Organization, name);
        if (builder.isIdMapped()) {

            organizationReference
                    = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, acuteHL7Helper);
        }
        builder.setServiceProvider(organizationReference);

        if ((encounterInd) && (!patientVisitId.isEmpty())) {
            Reference parentEncounter
                    = ReferenceHelper.createReference(ResourceType.Encounter, patientVisitId);

            parentEncounter
                    = IdHelper.convertLocallyUniqueReferenceToEdsReference(parentEncounter, acuteHL7Helper);

            builder.setPartOf(parentEncounter);
        }

    }

    /**
     *
     * @param pv1
     * @param existingParentEncounterBuilder
     * @param fhirResourceFiler
     * @param acuteHL7Helper
     * @throws Exception
     */
    private static void createChildEncounters(PV1 pv1,EVN evn, CX patientAccountNumber, EncounterBuilder existingParentEncounterBuilder, FhirResourceFiler fhirResourceFiler, AcuteHL7Helper acuteHL7Helper, String msgType, String patientGuid, String odsCode, String msgId) throws Exception {

        List<String> encounterIds = new ArrayList<String>();
        if("E".equalsIgnoreCase(String.valueOf(pv1.getPatientClass()))) {
            if ("A03".equalsIgnoreCase(msgType)) {
                encounterIds.add(pv1.getVisitNumber().getID() + ":30:EM");
            }  else if ("A02".equalsIgnoreCase(msgType)) {
                encounterIds.add(pv1.getVisitNumber().getID() + ":20:EM");
            }  else if ("A08".equalsIgnoreCase(msgType)) {
                Encounter existChildEnctDisc = (Encounter) acuteHL7Helper.retrieveResource(pv1.getVisitNumber().getID() + ":30:EM", ResourceType.Encounter);
                encounterIds.add(pv1.getVisitNumber().getID() + ":10:EM");
                if(existChildEnctDisc != null) {
                    encounterIds.add(pv1.getVisitNumber().getID() + ":30:EM");
                }
            }
            else {
                encounterIds.add(pv1.getVisitNumber().getID() + ":10:EM");
            }

        } else if("I".equalsIgnoreCase(String.valueOf(pv1.getPatientClass()))) {
            if ("A03".equalsIgnoreCase(msgType)) {
                encounterIds.add(pv1.getVisitNumber().getID() + ":30:IP");

            } else if ("A02".equalsIgnoreCase(msgType)) {
                String sequence = getNextUniqueSequenceId(acuteHL7Helper,pv1,"IP");
                encounterIds.add(pv1.getVisitNumber().getID() + sequence);
            } else if ("A08".equalsIgnoreCase(msgType)) {
                encounterIds.add(pv1.getVisitNumber().getID() + ":10:IP");
//                String exstChildEnctSequence = getlatestUniqueSequenceId(acuteHL7Helper,pv1, "IP");
//                if(exstChildEnctSequence != null) {
//                    encounterIds.add(pv1.getVisitNumber().getID() + exstChildEnctSequence);
//                }
                Encounter exstChildEnctDisc = (Encounter) acuteHL7Helper.retrieveResource(pv1.getVisitNumber().getID() + ":30:IP", ResourceType.Encounter);
                if(exstChildEnctDisc != null) {
                    encounterIds.add(pv1.getVisitNumber().getID() + ":30:IP");
                }

            }
            else if("A12".equalsIgnoreCase(msgType)){
                String latestChildEncId = pv1.getVisitNumber().getID() + getlatestUniqueSequenceId(acuteHL7Helper,pv1,"IP");
                Encounter exstChildEnctTrans = (Encounter) acuteHL7Helper.retrieveResource(latestChildEncId, ResourceType.Encounter);
                if(exstChildEnctTrans !=null){
                    EncounterBuilder cBuilder = new EncounterBuilder(exstChildEnctTrans);
                    fhirResourceFiler.deletePatientResource(null, false, cBuilder);
                    fhirResourceFiler.savePatientResource(null, !existingParentEncounterBuilder.isIdMapped(), existingParentEncounterBuilder);
                }
            }
            else {
                encounterIds.add(pv1.getVisitNumber().getID() + ":10:IP");
            }

        } else if("O".equalsIgnoreCase(String.valueOf(pv1.getPatientClass()))) {
            encounterIds.add(pv1.getVisitNumber().getID() + ":10:OP");
        }

        if(encounterIds != null) {
            for(String encounterId : encounterIds) {
                ContainedListBuilder existingEncounterList = new ContainedListBuilder(existingParentEncounterBuilder);

                EncounterBuilder childEncounterBuilder = new EncounterBuilder();

                if("E".equalsIgnoreCase(String.valueOf(pv1.getPatientClass()))) {
                    childEncounterBuilder.setClass(Encounter.EncounterClass.EMERGENCY);
                    CodeableConceptBuilder codeableConceptBuilderAdmission
                            = new CodeableConceptBuilder(childEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
                    if ("A03".equalsIgnoreCase(msgType)) {
                        codeableConceptBuilderAdmission.setText("Emergency Discharge");

                    }else if ("A02".equalsIgnoreCase(msgType)){
                        codeableConceptBuilderAdmission.setText("Emergency Transfer");
                    }else {
                        codeableConceptBuilderAdmission.setText("Emergency Admission");
                    }

                } else if("I".equalsIgnoreCase(String.valueOf(pv1.getPatientClass()))) {
                    childEncounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);
                    CodeableConceptBuilder codeableConceptBuilderAdmission
                            = new CodeableConceptBuilder(childEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
                    if ("A03".equalsIgnoreCase(msgType)) {
                        codeableConceptBuilderAdmission.setText("Inpatient Discharge");

                    } else if ("A02".equalsIgnoreCase(msgType)){
                        codeableConceptBuilderAdmission.setText("Inpatient Transfer");
                    } else {
                        codeableConceptBuilderAdmission.setText("Inpatient Admission");
                    }

                } else if("O".equalsIgnoreCase(String.valueOf(pv1.getPatientClass()))) {
                    childEncounterBuilder.setClass(Encounter.EncounterClass.OUTPATIENT);

                    CodeableConceptBuilder codeableConceptBuilderAdmission
                            = new CodeableConceptBuilder(childEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
                    codeableConceptBuilderAdmission.setText("Outpatient Attendance");
                }

                childEncounterBuilder.setId(encounterId);

                TS clinicalDtTime = null;
                if("10".equals(encounterId.split(":")[1])){
                    clinicalDtTime = pv1.getAdmitDateTime();
                }
                else if("20".equals(encounterId.split(":")[1])){
                    clinicalDtTime = evn.getEvn6_EventOccurred();
                }
                else if("30".equals(encounterId.split(":")[1])){
                    clinicalDtTime = pv1.getDischargeDateTime();
                }
                TS dischargeDtTime = pv1.getDischargeDateTime();
                Date dsDt=null;
                if ( !clinicalDtTime.isEmpty() && clinicalDtTime !=null) {
                    String startDt = String.valueOf(clinicalDtTime.getTimeOfAnEvent());
                    DateFormat clinicalFormat = new SimpleDateFormat("yyyyMMddHHmmss");
                    clinicalFormat.setTimeZone(TimeZone.getTimeZone("Europe/London"));
                    Date stDt = clinicalFormat.parse(startDt);
                    childEncounterBuilder.setPeriodStart(stDt);

                    if (!dischargeDtTime.isEmpty()) {
                        String endDt = String.valueOf(dischargeDtTime.getTimeOfAnEvent());
                        DateFormat dischargeFormat = new SimpleDateFormat("yyyyMMddHHmmss");
                        dischargeFormat.setTimeZone(TimeZone.getTimeZone("Europe/London"));
                        dsDt = dischargeFormat.parse(endDt);
                    }
                }

                if (("A03".equalsIgnoreCase(msgType) && (!dischargeDtTime.isEmpty()))) {
                    childEncounterBuilder.setPeriodEnd(dsDt);
                    childEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
                } else {
                    childEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);
                }

                setCommonEncounterAttributes(pv1, childEncounterBuilder, acuteHL7Helper, patientGuid, true, odsCode);

                //add in additional extended data as Parameters resource with additional extension
                ContainedParametersBuilder containedParametersBuilder = new ContainedParametersBuilder(childEncounterBuilder);
                containedParametersBuilder.removeContainedParameters();

                if (patientAccountNumber!= null && patientAccountNumber.getID() != null && patientAccountNumber.getID().getValue() !=null) {
                    String finNumber = patientAccountNumber.getID().getValue();
                    //these mapping values will need establishing as actuals in IM
                    MapResponse propertyResponse = AcuteHL7Helper.retrieveIMLookupMap(odsCode,"fin_number",null);
                    String propertyCode = propertyResponse.getConcept().getCode();
                    String propertyName = "TEXT_"+propertyCode;
                    containedParametersBuilder.addParameter(propertyName, finNumber);
                }

                String encounterType = pv1.getPatientType().getValue();
                if (!Strings.isNullOrEmpty(encounterType)  && !encounterType.equalsIgnoreCase("\"\"")) {

                    MapResponse propertyResponse = AcuteHL7Helper.retrieveIMLookupMap(odsCode,"encounter_type",null);
                    MapResponse valueResponse = AcuteHL7Helper.retrieveIMLookupMap(odsCode,"encounter_type",encounterType);

                    if(valueResponse.getWasCreated()) {
                        AcuteHL7Helper.raiseMissingAlert(encounterType, "encounter_type", odsCode,msgType, acuteHL7Helper.getExchangeId().toString(), msgId,patientGuid, acuteHL7Helper);
                    }

                    CodeableConcept ccValue = new CodeableConcept();
                    ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                            .setSystem(valueResponse.getConcept().getScheme());

                    containedParametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
                }


                String treatmentFunctionCode = pv1.getHospitalService().getValue();
                if (!Strings.isNullOrEmpty(treatmentFunctionCode)  && !treatmentFunctionCode.equalsIgnoreCase("\"\"")) {

                    MapResponse propertyResponse = AcuteHL7Helper.retrieveIMLookupMap(odsCode,"treatment_function_code",null);
                    MapResponse valueResponse = AcuteHL7Helper.retrieveIMLookupMap(odsCode,"treatment_function_code",treatmentFunctionCode);

                    if(valueResponse.getWasCreated()) {
                        AcuteHL7Helper.raiseMissingAlert(treatmentFunctionCode, "treatment_function_code", odsCode,msgType,acuteHL7Helper.getExchangeId().toString(),msgId,patientGuid,acuteHL7Helper);
                    }

                    CodeableConcept ccValue = new CodeableConcept();
                    ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                            .setSystem(valueResponse.getConcept().getScheme());

                    containedParametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
                }

                String admissionSourceCode = pv1.getAdmitSource().getValue();
                if (!Strings.isNullOrEmpty(admissionSourceCode) && !admissionSourceCode.equalsIgnoreCase("\"\"")) {

                    MapResponse propertyResponse = AcuteHL7Helper.retrieveIMLookupMap(odsCode,"admission_source_code",null);
                    MapResponse valueResponse = AcuteHL7Helper.retrieveIMLookupMap(odsCode,"admission_source_code",admissionSourceCode);

                    if(valueResponse.getWasCreated()) {
                        AcuteHL7Helper.raiseMissingAlert(admissionSourceCode, "admission_source_code", odsCode,msgType,acuteHL7Helper.getExchangeId().toString(),msgId,patientGuid,acuteHL7Helper);
                    }

                    CodeableConcept ccValue = new CodeableConcept();
                    ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                            .setSystem(valueResponse.getConcept().getScheme());
                    containedParametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
                }

                String admissionMethodCode = pv1.getAdmissionType().getValue();
                if (!Strings.isNullOrEmpty(admissionMethodCode) && !admissionMethodCode.equalsIgnoreCase("\"\"")) {

                    MapResponse propertyResponse = AcuteHL7Helper.retrieveIMLookupMap(odsCode,"admission_method_code",null);
                    MapResponse valueResponse = AcuteHL7Helper.retrieveIMLookupMap(odsCode,"admission_method_code",admissionMethodCode);

                    if(valueResponse.getWasCreated()) {
                        AcuteHL7Helper.raiseMissingAlert(admissionMethodCode, "admission_method_code", odsCode,msgType,acuteHL7Helper.getExchangeId().toString(),msgId,patientGuid,acuteHL7Helper);
                    }

                    CodeableConcept ccValue = new CodeableConcept();
                    ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                            .setSystem(valueResponse.getConcept().getScheme());
                    containedParametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
                }
                if ("A03".equalsIgnoreCase(msgType)) {
                    String dischargeDisposition = pv1.getDischargeDisposition().getValue();
                    if (!Strings.isNullOrEmpty(dischargeDisposition) && !dischargeDisposition.equalsIgnoreCase("\"\"")) {

                        MapResponse propertyResponse = AcuteHL7Helper.retrieveIMLookupMap(odsCode,"discharge_method",null);
                        MapResponse valueResponse = AcuteHL7Helper.retrieveIMLookupMap(odsCode,"discharge_method",dischargeDisposition);

                        if(valueResponse.getWasCreated()) {
                            AcuteHL7Helper.raiseMissingAlert(dischargeDisposition, "discharge_method", odsCode,msgType,acuteHL7Helper.getExchangeId().toString(),msgId,patientGuid,acuteHL7Helper);
                        }

                        CodeableConcept ccValue = new CodeableConcept();
                        ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                                .setSystem(valueResponse.getConcept().getScheme());
                        containedParametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
                    }

                    String dischargedToLocation = pv1.getDischargedToLocation().getCm_dld1_DischargeLocation().toString();
                    if (!Strings.isNullOrEmpty(dischargedToLocation) && !dischargedToLocation.equalsIgnoreCase("\"\"")) {

                        MapResponse propertyResponse = AcuteHL7Helper.retrieveIMLookupMap(odsCode,"discharge_destination_code",null);
                        MapResponse valueResponse = AcuteHL7Helper.retrieveIMLookupMap(odsCode,"discharge_destination_code",dischargedToLocation);

                        if(valueResponse.getWasCreated()) {
                            AcuteHL7Helper.raiseMissingAlert(dischargedToLocation, "discharge_destination_code", odsCode,msgType,acuteHL7Helper.getExchangeId().toString(),msgId,patientGuid,acuteHL7Helper);
                        }

                        CodeableConcept ccValue = new CodeableConcept();
                        ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                                .setSystem(valueResponse.getConcept().getScheme());
                        containedParametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
                    }
                }

                //and link the parent to this new child encounter
                Reference childDischargeRef = ReferenceHelper.createReference(ResourceType.Encounter, encounterId);
                if (existingParentEncounterBuilder.isIdMapped()) {
                    childDischargeRef
                            = IdHelper.convertLocallyUniqueReferenceToEdsReference(childDischargeRef, acuteHL7Helper);
                }
                existingEncounterList.addReference(childDischargeRef);

                fhirResourceFiler.savePatientResource(null, !existingParentEncounterBuilder.isIdMapped(), existingParentEncounterBuilder);
                fhirResourceFiler.savePatientResource(null, childEncounterBuilder);
            }
        }
    }

    /**
     *
     * @param pv1
     * @param fhirResourceFiler
     * @param acuteHL7Helper
     * @param msgType
     * @throws Exception
     */
    public static void deleteEncounterAndChildren(PV1 pv1, FhirResourceFiler fhirResourceFiler, AcuteHL7Helper acuteHL7Helper, String msgType, String msgId) throws Exception {
        //retrieve the existing Top level parent Encounter resource to perform a deletion plus any child encounters
        Encounter existingParentEncounter
                = (Encounter) acuteHL7Helper.retrieveResourceForLocalId(ResourceType.Encounter, String.valueOf(pv1.getVisitNumber().getID()));

        EncounterBuilder parentEncounterBuilder
                = new EncounterBuilder(existingParentEncounter);

        if (existingParentEncounter!= null && existingParentEncounter.hasContained()) {
            ContainedListBuilder listBuilder = new ContainedListBuilder(parentEncounterBuilder);
            ResourceDalI resourceDal = DalProvider.factoryResourceDal();

            for (List_.ListEntryComponent item : listBuilder.getContainedListItems()) {
                Reference ref = item.getItem();

                ReferenceComponents comps = ReferenceHelper.getReferenceComponents(ref);
                if (comps.getResourceType() != ResourceType.Encounter) {
                    continue;
                }
                Encounter childEncounter
                        = (Encounter) resourceDal.getCurrentVersionAsResource(acuteHL7Helper.getServiceId(), ResourceType.Encounter, comps.getId());
                if (childEncounter != null) {
                    boolean deleteChild = false;
                    if(("EMERGENCY".equalsIgnoreCase(childEncounter.getClass_().toString())) && ("A11".equalsIgnoreCase(msgType))) {
                        deleteChild = true;

                    }  else if(("INPATIENT".equalsIgnoreCase(childEncounter.getClass_().toString())) && ("A11".equalsIgnoreCase(msgType))) {
                        deleteChild = true;

                    } else if("OUTPATIENT".equalsIgnoreCase(childEncounter.getClass_().toString())) {
                        deleteChild = true;
                    }

                    if(deleteChild) {
                        LOG.debug("Deleting child encounter " + childEncounter.getId());
                        fhirResourceFiler.deletePatientResource(null, false, new EncounterBuilder(childEncounter));
                    }

                }
            }
            //finally, delete the top level parent
            if ("A11".equalsIgnoreCase(msgType)) {
                fhirResourceFiler.deletePatientResource(null, false, parentEncounterBuilder);
            }

        } else {

            TransformWarnings.log(LOG, acuteHL7Helper,
                    "Cannot find existing Encounter  message type={}, Segment={}, EncounterID={}, MessageID={}",
                    msgType,"Cannot find existing Encounter: {} for deletion",String.valueOf(pv1.getVisitNumber().getID()),msgId);
        }
    }
    public static void undoChildEncounter(Encounter existingParentEncounter, AcuteHL7Helper acuteHL7Helper, FhirResourceFiler fhirResourceFiler) throws Exception {
        EncounterBuilder parentEncounterBuilder = new EncounterBuilder(existingParentEncounter);

        ContainedListBuilder listBuilder = new ContainedListBuilder(parentEncounterBuilder);
        ResourceDalI resourceDal = DalProvider.factoryResourceDal();

        for (List_.ListEntryComponent item : listBuilder.getContainedListItems()) {

            Reference ref = item.getItem();
            ReferenceComponents comps = ReferenceHelper.getReferenceComponents(ref);
            if (comps.getResourceType() != ResourceType.Encounter) {
                continue;
            }
            Encounter childEncounter
                    = (Encounter) resourceDal.getCurrentVersionAsResource(acuteHL7Helper.getServiceId(), ResourceType.Encounter, comps.getId());

            if (childEncounter != null) {
                EncounterBuilder cBuilder = new EncounterBuilder(childEncounter);

                if (listBuilder.getContainedListItems().size() == 1) {
                    cBuilder.getPeriod().setEnd(null);
                    fhirResourceFiler.savePatientResource(null, false, cBuilder);
                } else {
                    if (cBuilder.getPeriod()!=null && cBuilder.getPeriod().getEnd()!=null) {
                        fhirResourceFiler.deletePatientResource(null, false, cBuilder);
                    }
                }
            }
        }
        EncounterTransformer.deleteEndDate(existingParentEncounter, fhirResourceFiler);

    }
    public static String getNextUniqueSequenceId(AcuteHL7Helper acuteHL7Helper, PV1 pv1 , String patientClass) throws Exception {
        String ret = null;
        int count=1;
        while(count>0){
            Encounter existChildEnctTrans = (Encounter) acuteHL7Helper.retrieveResource(pv1.getVisitNumber().getID() + ":20:"+String.format("%02d", count)+":" + patientClass, ResourceType.Encounter);
            if(existChildEnctTrans == null){
                ret = ":20:"+String.format("%02d", count)+":" + patientClass;
                break;
            }
            count++;
        }
        return ret;
    }

    public static String getlatestUniqueSequenceId(AcuteHL7Helper acuteHL7Helper, PV1 pv1, String patientClass) throws Exception {
        String ret = null;
        int count=1;
        while(count>0){
            Encounter existChildEnctTrans = (Encounter) acuteHL7Helper.retrieveResource(pv1.getVisitNumber().getID() + ":20:"+String.format("%02d", count)+":" + patientClass, ResourceType.Encounter);
            if(existChildEnctTrans == null){
                break;
            }
            ret = ":20:"+String.format("%02d", count)+":" + patientClass;
            count++;
        }
        return ret;
    }
}
