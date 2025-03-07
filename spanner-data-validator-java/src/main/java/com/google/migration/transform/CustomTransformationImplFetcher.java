package com.google.migration.transform;

import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomTransformationImplFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(CustomTransformationImplFetcher.class);
  private static ISpannerMigrationTransformer spannerMigrationTransformer = null;

  public static synchronized ISpannerMigrationTransformer getCustomTransformationLogicImpl(
      CustomTransformation customTransformation) {

    if (spannerMigrationTransformer == null) {
      spannerMigrationTransformer = getApplyTransformationImpl(customTransformation);
    }
    return spannerMigrationTransformer;
  }

  public static ISpannerMigrationTransformer getApplyTransformationImpl(
      CustomTransformation customTransformation) {
    if (customTransformation == null
        || customTransformation.jarPath() == null
        || customTransformation.classPath() == null) {
      return null;
    }
    if (!customTransformation.jarPath().isEmpty() && !customTransformation.classPath().isEmpty()) {
      LOG.info(
          "Getting spanner migration transformer : "
              + customTransformation.jarPath()
              + " with class: "
              + customTransformation.classPath());
      try {
        // Get the start time of loading the custom class
        Instant startTime = Instant.now();

        // Getting the jar URL which contains target class
        URL[] classLoaderUrls = JarFileReader.saveFilesLocally(customTransformation.jarPath());

        // Create a new URLClassLoader
        URLClassLoader urlClassLoader = new URLClassLoader(classLoaderUrls);

        // Load the target class
        Class<?> customTransformationClass =
            urlClassLoader.loadClass(customTransformation.classPath());

        // Create a new instance from the loaded class
        Constructor<?> constructor = customTransformationClass.getConstructor();
        ISpannerMigrationTransformer spannerMigrationTransformation =
            (ISpannerMigrationTransformer) constructor.newInstance();
        // Get the end time of loading the custom class
        Instant endTime = Instant.now();
        LOG.info(
            "Custom jar "
                + customTransformation.jarPath()
                + ": Took "
                + (new Duration(startTime, endTime)).toString()
                + " to load");
        LOG.info(
            "Invoking init of the custom class with input as {}",
            customTransformation.customParameters());
        spannerMigrationTransformation.init(customTransformation.customParameters());
        return spannerMigrationTransformation;
      } catch (Exception e) {
        throw new RuntimeException("Error loading custom class : " + e.getMessage());
      }
    }
    return null;
  }
}

