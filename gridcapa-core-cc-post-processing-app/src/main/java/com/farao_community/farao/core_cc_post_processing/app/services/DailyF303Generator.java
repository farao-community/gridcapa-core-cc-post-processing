/*
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import org.springframework.stereotype.Service;

/**
 * @author Pengbo Wang {@literal <pengbo.wang at rte-international.com>}
 * @author Mohamed BenRejeb {@literal <mohamed.ben-rejeb at rte-france.com>}
 * @author Baptiste Seguinot {@literal <baptiste.seguinot at rte-france.com}
 */
@Service
public class DailyF303Generator {

    private final MinioAdapter minioAdapter;

    public DailyF303Generator(MinioAdapter minioAdapter) {
        this.minioAdapter = minioAdapter;
    }

    /*public FlowBasedConstraintDocument generate(RaoIntegrationTask raoIntegrationTask) {

        try (InputStream cracXmlInputStream = minioAdapter.getInputStreamFromUrl(raoIntegrationTask.getInputCracXmlFileUrl())) {

            // get native CRAC
            FbConstraint nativeCrac = new FbConstraintImporter().importNativeCrac(cracXmlInputStream);

            // generate F303Info for each 24 hours of the initial CRAC
            Map<Integer, Interval> positionMap = IntervalUtil.getPositionsMap(nativeCrac.getDocument().getConstraintTimeInterval().getV());
            List<HourlyF303Info> hourlyF303Infos = new ArrayList<>();
            positionMap.values().forEach(interval -> hourlyF303Infos.add(new HourlyF303InfoGenerator(nativeCrac, interval, raoIntegrationTask, minioAdapter).generate()));

            // gather hourly info in one common document, cluster the elements that can be clusterized
            FlowBasedConstraintDocument flowBasedConstraintDocument = new DailyF303Clusterizer(hourlyF303Infos, nativeCrac).generateClusterizedDocument();

            // save this to fill in rao response
            raoIntegrationTask.getDailyOutputs().setOutputFlowBasedConstraintDocumentMessageId(flowBasedConstraintDocument.getDocumentIdentification().getV());
            return flowBasedConstraintDocument;
        } catch (Exception e) {
            throw new RaoIntegrationException("Exception occurred during F303 file creation", e);
        }
    }*/
}
