/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.command

import io.micronaut.context.annotation.Factory
import io.micronaut.context.env.Environment
import jakarta.inject.Singleton
import java.util.EnumSet

/**
 * An enum of all feature flags, currently these are set via environment vars.
 *
 * Micronaut can inject a Set<FeatureFlag> singleton of all active feature flags.
 */
enum class FeatureFlag(
    val micronautEnvironmentName: String,
    private val envVar: EnvVar,
    private val predicate: (String) -> Boolean,
) {

    /** [AIRBYTE_CLOUD_DEPLOYMENT] is active when the connector is running in Airbyte Cloud. */
    AIRBYTE_CLOUD_DEPLOYMENT(
        micronautEnvironmentName = AIRBYTE_CLOUD_ENV,
        envVar = EnvVar.DEPLOYMENT_MODE,
        predicate = { it.lowercase() == "cloud" },
    );

    private enum class EnvVar(val defaultValue: String = "") {
        DEPLOYMENT_MODE
    }

    companion object {
        internal fun active(systemEnv: Map<String, String>): List<FeatureFlag> =
            entries.filter { featureFlag: FeatureFlag ->
                val envVar: EnvVar = featureFlag.envVar
                val envVarValue: String = systemEnv[envVar.name] ?: envVar.defaultValue
                featureFlag.predicate(envVarValue)
            }
    }

    @Factory
    private class MicronautFactory {

        @Singleton
        fun active(environment: Environment): Set<FeatureFlag> =
            EnumSet.noneOf(FeatureFlag::class.java).apply {
                addAll(
                    FeatureFlag.entries.filter {
                        environment.activeNames.contains(it.micronautEnvironmentName)
                    }
                )
            }
    }
}

const val AIRBYTE_CLOUD_ENV = "airbyte-cloud"
