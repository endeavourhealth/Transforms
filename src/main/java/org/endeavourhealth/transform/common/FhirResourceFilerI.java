package org.endeavourhealth.transform.common;

import org.endeavourhealth.transform.common.resourceBuilders.ResourceBuilderBase;

import java.util.UUID;

public interface FhirResourceFilerI {
    final UUID serviceId = null;
    final UUID systemId = null;

    /*public void saveAdminResource(CsvCurrentState parserState, Resource... resources) throws Exception;
    public void saveAdminResource(CsvCurrentState parserState, boolean mapIds, Resource... resources) throws Exception;
    public void deleteAdminResource(CsvCurrentState parserState, Resource... resources) throws Exception;
    public void deleteAdminResource(CsvCurrentState parserState, boolean mapIds, Resource... resources) throws Exception;
    public void savePatientResource(CsvCurrentState parserState, Resource... resources) throws Exception;
    public void savePatientResource(CsvCurrentState parserState, boolean mapIds, Resource... resources) throws Exception;
    public void deletePatientResource(CsvCurrentState parserState, Resource... resources) throws Exception;
    public void deletePatientResource(CsvCurrentState parserState, boolean mapIds, Resource... resources) throws Exception;*/

    public void saveAdminResource(CsvCurrentState parserState, ResourceBuilderBase... resources) throws Exception;

    public void saveAdminResource(CsvCurrentState parserState, boolean mapIds, ResourceBuilderBase... resources) throws Exception;

    public void deleteAdminResource(CsvCurrentState parserState, ResourceBuilderBase... resources) throws Exception;

    public void deleteAdminResource(CsvCurrentState parserState, boolean mapIds, ResourceBuilderBase... resources) throws Exception;

    public void savePatientResource(CsvCurrentState parserState, ResourceBuilderBase... resources) throws Exception;

    public void savePatientResource(CsvCurrentState parserState, boolean mapIds, ResourceBuilderBase... resources) throws Exception;

    public void deletePatientResource(CsvCurrentState parserState, ResourceBuilderBase... resources) throws Exception;

    public void deletePatientResource(CsvCurrentState parserState, boolean mapIds, ResourceBuilderBase... resources) throws Exception;

    //public static ExchangeBatch createExchangeBatch(UUID exchangeId, UUID edsPatientId) ;
    //  public void waitToFinish() throws Exception ;
    //    public static boolean isPatientResource(Resource resource) ;

    //  public static boolean isPatientResource(ResourceType type) ;

    public UUID getServiceId();

    public UUID getSystemId();

    public void logTransformRecordError(Throwable ex, CsvCurrentState state) throws Exception;

    void failIfAnyErrors() throws Exception;

}
