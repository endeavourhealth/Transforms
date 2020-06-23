package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.v23.datatype.TS;
import ca.uhn.hl7v2.model.v23.segment.PV1;
import org.hl7.fhir.instance.model.Encounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class EncounterTransformer {


    private static final Logger LOG = LoggerFactory.getLogger(EncounterTransformer.class);

    /**
     *
     * @param pv1
     * @param encounter
     * @return
     * @throws Exception
     */
    public static Encounter transformPV1ToEncounter(PV1 pv1, Encounter encounter) throws Exception {
        /*PL assignedPatientLoc = pv1.getAssignedPatientLocation();
        IS accStatus = pv1.getAccountStatus();*/
        TS admitDtTime = pv1.getAdmitDateTime();
        String startDt = String.valueOf(admitDtTime.getTimeOfAnEvent());
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        Date stDt = formatter.parse(startDt.substring(0,4)+"-"+startDt.substring(4,6)+"-"+startDt.substring(6,8));

        /*TS[] dischargeDtTime = pv1.getDischargeDateTime();
        String endDt = String.valueOf(dischargeDtTime[0].getTime());
        Date dsDt = formatter.parse(endDt.substring(0,4)+"-"+endDt.substring(4,6)+"-"+endDt.substring(6,8));*/

        /*IS patientType = pv1.getPatientType();
        IS servicingFacility = pv1.getServicingFacility();*/

        encounter.getMeta().addProfile("http://endeavourhealth.org/fhir/StructureDefinition/primarycare-encounter");

        /*if (String.valueOf(accStatus).equalsIgnoreCase("active")) {*/
        encounter.setStatus(Encounter.EncounterState.INPROGRESS);
        /*} else {
            encounter.setStatus(Encounter.EncounterState.FINISHED);
        }*/
        encounter.getPeriod().setStart(stDt);
        /*encounter.getPeriod().setEnd(dsDt);*/
        return encounter;

    }

}
