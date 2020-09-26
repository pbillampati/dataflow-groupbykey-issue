package com.dataflow.groupbykey.issue.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataFlowTemplateGenerator {

    private static final Logger logger = LoggerFactory.getLogger(DataFlowTemplateGenerator.class);


    public static void main(String... a) {
        String[] args = new String[]{
            "--project=replacewithprojectid",
            "--stagingLocation=replacewithstoragefolder",
            "--subnetwork=regions/us-central1/subnetworks/default-central1",
            "--usePublicIps=false",
            "--streaming=true",
            "--region=us-central1",
            "--diskSizeGb=30",
            "--appName=DataflowGroupByKeyJob",
            "--templateLocation=replacewithtemplatelocationandjsonname",
            "--gcpTempLocation=replacewithstoragefolder",
            "--runner=DataflowRunner",
        };
        logger.info("Start generating template");
        DataflowGroupByKeyJob.main(args);
        logger.info("End generating template");
    }
}