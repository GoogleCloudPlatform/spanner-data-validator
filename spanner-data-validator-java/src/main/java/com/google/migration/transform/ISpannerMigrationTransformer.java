package com.google.migration.transform;

import com.google.migration.exceptions.InvalidTransformationException;

public interface ISpannerMigrationTransformer {
  void init(String customParameters);

  MigrationTransformationResponse toSpannerRow(MigrationTransformationRequest request)
      throws InvalidTransformationException;

  MigrationTransformationResponse toSourceRow(MigrationTransformationRequest request)
      throws InvalidTransformationException;

}
