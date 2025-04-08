package com.google.migration.transform;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.common.base.Strings;
import java.io.Serializable;
import javax.annotation.Nullable;

@AutoValue
public abstract class CustomTransformation implements Serializable {
  public abstract String jarPath();

  public abstract String classPath();

  @Nullable
  public abstract String customParameters();

  public static CustomTransformation.Builder builder(String jarPath, String classPath) {
    return new AutoValue_CustomTransformation.Builder().setJarPath(jarPath).setClassPath(classPath);
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract CustomTransformation.Builder setJarPath(String jarPath);

    public abstract CustomTransformation.Builder setClassPath(String classPath);

    public abstract CustomTransformation.Builder setCustomParameters(String customParameters);

    abstract CustomTransformation autoBuild();

    public CustomTransformation build() {

      CustomTransformation customTransformation = autoBuild();
      checkState(
          (Strings.isNullOrEmpty(customTransformation.jarPath()))
              == (Strings.isNullOrEmpty(customTransformation.classPath())),
          "Both jarPath and classPath must be set or both must be empty/null.");
      return customTransformation;
    }
  }
}
