package com.dataflow.groupbykey.issue.job;

import java.io.Serializable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions.CheckEnabled;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataflowGroupByKeyJob implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(DataflowGroupByKeyJob.class);

    public interface Options extends PipelineOptions, StreamingOptions {

        ValueProvider<String> getSubscriptionName();

        void setSubscriptionName(ValueProvider<String> value);
    }

    public static void main(String... args) {
        logger.info("DataflowGroupByKeyJob Entry");
        DataflowGroupByKeyJob job = new DataflowGroupByKeyJob();
        job.setupAndRunPipeline(args);
    }

    protected void setupAndRunPipeline(String[] args) {
        try {
            PipelineOptionsFactory.register(Options.class);
            Options options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(Options.class);

            Pipeline pipeline = Pipeline.create(options);
            pipeline.getOptions().setStableUniqueNames(CheckEnabled.WARNING);

            PCollection<String> inputMsgs = getReadPubSubSubscription(options, pipeline);

            PCollection<String> windowedMessages = inputMsgs.apply(Window.<String>into(FixedWindows
                .of(Duration.standardMinutes(5))).discardingFiredPanes());
            PCollection<KV<String, String>> passThrough1Msgs = windowedMessages
                .apply("PassThrough1",
                    ParDo.of(new DoFn<String, KV<String, String>>() {
                        @ProcessElement
                        public void processElement(@Element String msg, ProcessContext context) {
                            logger.info("Incoming message: {}", msg);
                            context.output(KV.of("12#123", msg));
                        }
                    }));

            PCollection<KV<String, Iterable<String>>> groupedMsgs =
                passThrough1Msgs.apply("GroupKey",
                    GroupByKey.<String, String>create());

            PCollection<String> results = groupedMsgs
                .apply("GetChildsForEachGroup",
                    ParDo.of(new DoFn<KV<String, Iterable<String>>, String>() {
                        @ProcessElement
                        public void processElement(@Element KV<String, Iterable<String>> msg,
                            ProcessContext context) {
                            String value = msg.getValue().iterator().next();
                            logger.info("Processing Group : {} value {}", msg.getKey(), value);

                            for (int i = 0; i < 800000; i++) {
                                context.output(value.concat(value).concat("-" + i));
                            }
                        }
                    }));

            results.apply("Reshuffle", Reshuffle.viaRandomKey());

            pipeline.run();
        } catch (Exception e) {
            logger.error("DataflowGroupByKeyJob has failed", e);
        }
    }


    protected PCollection<String> getReadPubSubSubscription(
        Options options, Pipeline pipeline) {
        return pipeline.apply("ReadPubSubSubscription",
            PubsubIO.readStrings().fromSubscription(options.getSubscriptionName()));
    }

}
