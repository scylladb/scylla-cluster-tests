#!groovy

// The latest GA scylla release, the same way the web installer (get.scylladb.com/server) and
// sdcm.utils.common.get_latest_scylla_release() find it: the newest GA patch of the newest release
// series published to the downloads bucket.
String call() {
    String bucket = 'https://s3.amazonaws.com/downloads.scylladb.com'
    String prefix = 'downloads/scylla/relocatable'
    for (String series : releaseSeries(listBucket(bucket, "${prefix}/scylladb-"), prefix)) {
        String patch = latestGaPatch(listBucket(bucket, "${prefix}/scylladb-${series}/00-Build-${series}."), series)
        if (patch) {
            return series + '.' + patch
        }
    }
    error("No GA scylla release found under ${bucket}/${prefix}")
}

String listBucket(String bucket, String prefix) {
    return sh(script: "curl -sSf --retry 3 --max-time 30 '${bucket}?prefix=${prefix}&delimiter=/'", returnStdout: true)
}

// The release series (e.g. 2026.1) in a bucket listing, newest first.
@NonCPS
List<String> releaseSeries(String listing, String prefix) {
    List<String> series = (listing =~ "<Prefix>${prefix}/scylladb-(20\\d{2}\\.\\d+)/</Prefix>")
        .collect { it[1] }.unique()
    // Compare numerically, so 2026.10 sorts above 2026.9
    return series.sort { a, b ->
        List<Integer> x = a.tokenize('.')*.toInteger()
        List<Integer> y = b.tokenize('.')*.toInteger()
        (y[0] <=> x[0]) ?: (y[1] <=> x[1])
    }
}

// The highest GA patch number of a series in its 00-Build marker listing, or null if it has none.
@NonCPS
String latestGaPatch(String listing, String series) {
    // GA markers only: an RC marker is 00-Build-<series>.<patch>~rc<N>-<build>.txt
    Integer latest = null
    for (Integer patch : (listing =~ "/00-Build-${series.replace('.', '\\.')}\\.(\\d+)-").collect { it[1].toInteger() }) {
        // Not Collection.max(): the script-security sandbox rejects it
        if (latest == null || patch > latest) {
            latest = patch
        }
    }
    return latest == null ? null : latest.toString()
}
