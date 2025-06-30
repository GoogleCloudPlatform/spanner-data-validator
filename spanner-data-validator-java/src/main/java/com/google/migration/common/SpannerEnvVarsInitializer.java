package com.google.migration.common;

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.harness.JvmInitializer;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

// https://medium.com/google-cloud/managing-worker-dependencies-in-apache-beam-pipelines-5ad0dd8d5be1
@AutoService(JvmInitializer.class)
public class SpannerEnvVarsInitializer implements JvmInitializer {
  public void beforeProcessing(@UnknownKeyFor @NonNull @Initialized PipelineOptions options) {
  }
}
