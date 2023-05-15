package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class FileExporterHelper {

    private final MinioAdapter minioAdapter;

    private static final Logger LOGGER = LoggerFactory.getLogger(FileExporterHelper.class);

    private static final String ALEGRO_GEN_BE = "XLI_OB1B_generator";
    private static final String ALEGRO_GEN_DE = "XLI_OB1A_generator";
    private static final String DOMAIN_ID = "10Y1001C--00059P";

    public FileExporterHelper(MinioAdapter minioAdapter) {
        this.minioAdapter = minioAdapter;
    }

    /*public void exportNetworkInTmpOutput(RaoIntegrationTask raoIntegrationTask, HourlyRaoResult hourlyRaoResult) throws IOException {
        LOGGER.info("RAO integration task: '{}', exporting uct network with pra for timestamp: '{}'", raoIntegrationTask.getTaskId(), hourlyRaoResult.getInstant());

        Network network;
        try (InputStream cgmInputStream = minioAdapter.getInputStreamFromUrl(hourlyRaoResult.getNetworkWithPraUrl())) {
            network = Network.read(minioAdapter.getFileNameFromUrl(hourlyRaoResult.getNetworkWithPraUrl()), cgmInputStream);
        }
        MemDataSource memDataSource = new MemDataSource();

        // work around until the problem of "Too many loads connected to this bus" is corrected
        removeVirtualLoadsFromNetwork(network);
        // work around until the problem of "Too many generators connected to this bus" is corrected
        removeAlegroVirtualGeneratorsFromNetwork(network);
        // work around until fictitious loads and generators are not created in groovy script anymore
        removeFictitiousGeneratorsFromNetwork(network);
        removeFictitiousLoadsFromNetwork(network);
        network.write("UCTE", new Properties(), memDataSource);
        String networkNewFileName = OutputFileNameUtil.generateUctFileName(hourlyRaoResult.getInstant(), raoIntegrationTask.getVersion());
        File targetFile = new File(raoIntegrationTask.getDailyOutputs().getNetworkTmpOutputsPath(), networkNewFileName); //NOSONAR

        try (InputStream is = memDataSource.newInputStream("", "uct")) {
            Files.copy(is, targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private void removeVirtualLoadsFromNetwork(Network network) {
        List<String> virtualLoadsList = new ArrayList<>();
        network.getSubstationStream().forEach(substation -> substation.getVoltageLevels()
                .forEach(voltageLevel -> voltageLevel.getBusBreakerView().getBuses()
                        .forEach(bus -> bus.getLoadStream().filter(busLoad -> busLoad.getNameOrId().contains("_virtualLoad")).forEach(virtualLoad -> virtualLoadsList.add(virtualLoad.getNameOrId()))
                        )));
        virtualLoadsList.forEach(virtualLoad -> network.getLoad(virtualLoad).remove());
    }

    private void removeAlegroVirtualGeneratorsFromNetwork(Network network) {
        Optional.ofNullable(network.getGenerator(ALEGRO_GEN_BE)).ifPresent(Generator::remove);
        Optional.ofNullable(network.getGenerator(ALEGRO_GEN_DE)).ifPresent(Generator::remove);
    }

    private void removeFictitiousGeneratorsFromNetwork(Network network) {
        Set<String> generatorsToRemove = network.getGeneratorStream().filter(Generator::isFictitious).map(Generator::getId).collect(Collectors.toSet());
        generatorsToRemove.forEach(id -> network.getGenerator(id).remove());
    }

    private void removeFictitiousLoadsFromNetwork(Network network) {
        Set<String> loadsToRemove = network.getLoadStream().filter(Load::isFictitious).map(Load::getId).collect(Collectors.toSet());
        loadsToRemove.forEach(id -> network.getLoad(id).remove());
    }

    Crac importCracFromHourlyRaoRequest(RaoIntegrationTask raoIntegrationTask, HourlyRaoResult raoResult) {
        HourlyRaoRequest hourlyRaoRequest = raoIntegrationTask.getHourlyRequestFromResponse(raoResult);
        String cracFileUrl = hourlyRaoRequest.getCracFileUrl();
        try (InputStream cracFileInputStream = minioAdapter.getInputStreamFromUrl(cracFileUrl)) {
            return CracImporters.importCrac(minioAdapter.getFileNameFromUrl(cracFileUrl), cracFileInputStream);
        } catch (Exception e) {
            throw new RaoIntegrationException(String.format("Exception occurred while importing CRAC file: %s. Cause: %s", minioAdapter.getFileNameFromUrl(cracFileUrl), e.getMessage()));
        }
    }

    public void exportCneInTmpOutput(RaoIntegrationTask raoIntegrationTask, HourlyRaoResult hourlyRaoResult) throws IOException {
        LOGGER.info("RAO integration task: '{}', creating CNE Result for timestamp: '{}'", raoIntegrationTask.getTaskId(), hourlyRaoResult.getInstant());
        //create CNE with input from inputNetwork, outputCracJson and inputCraxXml
        HourlyRaoRequest hourlyRaoRequest = raoIntegrationTask.getHourlyRaoRequests().stream().filter(request -> request.getInstant().equals(hourlyRaoResult.getInstant()))
                .findFirst().orElseThrow(() -> new RaoIntegrationException(String.format("Exception occurred while creating CNE file for timestamp %s. Cause: no rao result.", hourlyRaoResult.getInstant())));

        //get input network
        String networkFileUrl = hourlyRaoRequest.getNetworkFileUrl();
        Network network;
        try (InputStream networkInputStream = minioAdapter.getInputStreamFromUrl(networkFileUrl)) {
            network = Network.read(minioAdapter.getFileNameFromUrl(networkFileUrl), networkInputStream);
        }

        //import input crac xml file and get FbConstraintCreationContext
        String cracXmlFileUrl = raoIntegrationTask.getInputCracXmlFileUrl();
        FbConstraintCreationContext fbConstraintCreationContext;
        try (InputStream cracInputStream = minioAdapter.getInputStreamFromUrl(cracXmlFileUrl)) {
            fbConstraintCreationContext = CracHelper.importCracXmlGetFbInfoWithNetwork(hourlyRaoResult.getInstant(), network, cracInputStream);
        }

        //get crac from hourly inputs
        Crac cracJson = importCracFromHourlyRaoRequest(raoIntegrationTask, hourlyRaoResult);

        //get raoResult from result
        RaoResult raoResult;
        try (InputStream raoResultInputStream = minioAdapter.getInputStreamFromUrl(hourlyRaoResult.getRaoResultFileUrl())) {
            RaoResultImporter raoResultImporter = new RaoResultImporter();
            raoResult = raoResultImporter.importRaoResult(raoResultInputStream, cracJson);
        }
        //get raoParams from input
        RaoParameters raoParameters;
        try (InputStream raoParametersInputStream = minioAdapter.getInputStreamFromUrl(hourlyRaoRequest.getRaoParametersFileUrl())) {
            raoParameters = JsonRaoParameters.read(raoParametersInputStream);
        }

        //export CNE
        String cneNewFileName = OutputFileNameUtil.generateCneFileName(hourlyRaoResult.getInstant(), raoIntegrationTask);
        File targetFile = new File(raoIntegrationTask.getDailyOutputs().getCneTmpOutputsPath(), cneNewFileName); //NOSONAR

        try (FileOutputStream outputStreamCne = new FileOutputStream(targetFile)) {
            CoreCneExporter cneExporter = new CoreCneExporter();
            CneExporterParameters cneExporterParameters = getCneExporterParameters(raoIntegrationTask);
            cneExporter.exportCne(cracJson, network, fbConstraintCreationContext, raoResult, raoParameters, cneExporterParameters, outputStreamCne);

            //remember mrid f299 for f305 rao response payload
            hourlyRaoResult.setCneResultDocumentId(cneExporterParameters.getDocumentId());
        }
    }

    private CneExporterParameters getCneExporterParameters(RaoIntegrationTask raoIntegrationTask) {
        return new CneExporterParameters(
                generateCneMRID(raoIntegrationTask),
                raoIntegrationTask.getVersion(),
                DOMAIN_ID,
                CneExporterParameters.ProcessType.DAY_AHEAD_CC,
                RaoIXmlResponseGenerator.SENDER_ID,
                CneExporterParameters.RoleType.REGIONAL_SECURITY_COORDINATOR,
                RaoIXmlResponseGenerator.RECEIVER_ID,
                CneExporterParameters.RoleType.CAPACITY_COORDINATOR,
                raoIntegrationTask.getTimeInterval()
        );
    }

    private String generateCneMRID(RaoIntegrationTask raoIntegrationTask) {
        return String.format("%s-%s-F299v%s", RaoIXmlResponseGenerator.SENDER_ID, IntervalUtil.getFormattedBusinessDay(raoIntegrationTask.getTimeInterval()), raoIntegrationTask.getVersion());
    }

    public void exportMetadataFile(RaoIntegrationTask raoIntegrationTask, String targetMinioFolder, boolean isManualRun) {
        try {
            new RaoIMetadataGenerator(minioAdapter).exportMetadataFile(raoIntegrationTask, targetMinioFolder, isManualRun);
        } catch (Exception e) {
            LOGGER.error("Could not generate metadata file for rao task {}: {}", raoIntegrationTask.getTaskId(), e.getMessage());
        }
    }*/
}
