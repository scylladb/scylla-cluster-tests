#!groovy

/**
 * The `--export-cache <file>` option of `hydra load-config` for a stage that writes the configuration
 * cache: names the file in SCT_CONFIG_CACHE_FILE, or is empty when the pipeline leaves it unset.
 */
String call() {
    return env.SCT_CONFIG_CACHE_FILE ? "--export-cache ${env.SCT_CONFIG_CACHE_FILE}" : ""
}
