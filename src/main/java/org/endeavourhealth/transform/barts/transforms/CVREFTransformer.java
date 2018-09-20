package org.endeavourhealth.transform.barts.transforms;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.CernerCodeValueRefDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.barts.BartsCodeableConceptHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.schema.CVREF;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class CVREFTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(CVREFTransformer.class);

    private static CernerCodeValueRefDalI repository = DalProvider.factoryCernerCodeValueRefDal();

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser : parsers) {
            while (parser.nextRecord()) {
                //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
                //to parse any record in this file is a critical error. A bad entry here could have multiple serious effects
                transform((CVREF)parser, fhirResourceFiler, csvHelper);
            }
        }
    }


    public static void transform(CVREF parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {
        //For CVREF the first column should always resolve as a numeric code. We've seen some bad data appended to CVREF files
        if (!StringUtils.isNumeric(parser.getCodeValueCode().getString())) {
            return;
        }
        CsvCell codeValueCode = parser.getCodeValueCode();
        CsvCell date = parser.getDate();
        CsvCell activeInd = parser.getActiveInd();
        CsvCell codeDescTxt = parser.getCodeDescTxt();
        CsvCell codeDispTxt = parser.getCodeDispTxt();
        CsvCell codeMeaningTxt = parser.getCodeMeaningTxt();
        CsvCell codeSetNbr = parser.getCodeSetNbr();
        CsvCell codeSetDescTxt = parser.getCodeSetDescTxt();
        CsvCell aliasNhsCdAlias = parser.getAliasNhsCdAlias();

        //we need to handle multiple formats, so attempt to apply both formats here
        // Just to keep the log tidier from exceptions - if the date string has a period it's likely the bulk format
        Date formattedDate = null;
        if (!date.isEmpty()) {
            formattedDate = BartsCsvHelper.parseDate(date);
        }

        ResourceFieldMappingAudit auditWrapper = new ResourceFieldMappingAudit();

        auditWrapper.auditValue(codeValueCode.getRowAuditId(), codeValueCode.getColIndex(), BartsCodeableConceptHelper.CODE_VALUE);
        auditWrapper.auditValue(codeSetNbr.getRowAuditId(), codeSetNbr.getColIndex(), BartsCodeableConceptHelper.CODE_SET_NBR);
        auditWrapper.auditValue(codeDispTxt.getRowAuditId(), codeDispTxt.getColIndex(), BartsCodeableConceptHelper.DISP_TXT);
        auditWrapper.auditValue(codeDescTxt.getRowAuditId(), codeDescTxt.getColIndex(), BartsCodeableConceptHelper.DESC_TXT);
        auditWrapper.auditValue(codeMeaningTxt.getRowAuditId(), codeMeaningTxt.getColIndex(), BartsCodeableConceptHelper.MEANING_TXT);
        auditWrapper.auditValue(aliasNhsCdAlias.getRowAuditId(), aliasNhsCdAlias.getColIndex(), BartsCodeableConceptHelper.ALIAS_TXT);

        byte active = (byte) activeInd.getInt().intValue();
        CernerCodeValueRef mapping = new CernerCodeValueRef(codeValueCode.getString(),
                formattedDate,
                active,
                codeDescTxt.getString(),
                codeDispTxt.getString(),
                codeMeaningTxt.getString(),
                codeSetNbr.getLong(),
                codeSetDescTxt.getString(),
                aliasNhsCdAlias.getString(),
                fhirResourceFiler.getServiceId(),
                auditWrapper);


        //save to the DB
        repository.save(mapping, fhirResourceFiler.getServiceId());

        //LOG.debug("CVREF " + codeValueCode.getString() + " SET " + codeSetNbr.getString());
        //if it's a specialty, store the record as an Organization so we can refer to it from the Encounter resource
        if (codeSetNbr.getLong().equals(CodeValueSet.SPECIALITY)) {

            //LOG.debug("Specialty code");
            OrganizationBuilder organizationBuilder = new OrganizationBuilder();

            //use a special source ID so we don't mix up with any other Organization created from any other source
            Reference mainReference = csvHelper.createSpecialtyOrganisationReference(codeValueCode);
            String orgId = ReferenceHelper.getReferenceId(mainReference);
            organizationBuilder.setId(orgId, codeValueCode);

            organizationBuilder.setName(codeDispTxt.getString(), codeDispTxt);

            String parentId = csvHelper.findOrgRefIdForBarts();
            Reference reference = ReferenceHelper.createReference(ResourceType.Organization, parentId);
            organizationBuilder.setParentOrganisation(reference);

            fhirResourceFiler.saveAdminResource(parser.getCurrentState(), organizationBuilder);
            //LOG.debug("Saved organisation");
        }
    }

}
