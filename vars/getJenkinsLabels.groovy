#!groovy
import groovy.json.JsonSlurperClassic

def call(String backend, String region=null, String datacenter=null, String location=null, String oci_region=null ,Map overrides=null) {
    if (!(params instanceof Map)) {
        params = params.collectEntries()
    }
    if (overrides == null){
        overrides = [:]
    }
    // NOTE: `params` here has no `def`, so in a shared-library script this writes the build's
    // *global* binding and the merge below leaks out of this function. That makes an override
    // like `minicloud` *appear* to work as `params.minicloud` in a later `when {}` guard - do
    // not rely on it. Guards elsewhere must read a captured local. Left as-is rather than
    // localised because callers have depended on the leak for years.
    params += overrides // merge, overrides take precedence

    if (!backend) {
        backend = 'aws'
        println("Backend is null or empty, defaulting to 'aws'")
    }

    try {
        regionList = new JsonSlurperClassic().parseText(region)
        region = regionList[0]
    } catch(Exception) {

    }
    try {
        datacenterList = new JsonSlurperClassic().parseText(datacenter)
        datacenter = datacenterList[0]
    } catch(Exception) {

    }
    try {
        ociRegionList = new JsonSlurperClassic().parseText(oci_region)
        oci_region = ociRegionList[0]
    } catch(Exception) {

    }

    def gcp_project = params.gce_project?.trim() ?: 'gcp-sct-project-1'
    gcp_project = gcp_project == 'gcp' ? 'gcp-skilled-adapter-452' : gcp_project

    // NOTE: 'aws', 'gce' and 'oci' labels (without region) are useful for cases
    //       when backend-specific region param has no value.
    //       Even if test-case config has, it won't be accessible from here.
    //       So, apply default value for such cases.
    def jenkins_labels = ['aws-eu-west-1': 'aws-sct-builders-eu-west-1-v4-asg',
                          'aws-eu-west-2': 'aws-sct-builders-eu-west-2-v4-asg',
                          'aws-eu-north-1': 'aws-sct-builders-eu-north-1-v4-asg',
                          'aws-eu-central-1': 'aws-sct-builders-eu-central-1-v4-asg',
                          'aws-us-east-1' : 'aws-sct-builders-us-east-1-v4-asg',
                          'aws-us-west-2' : 'aws-sct-builders-us-west-2-v4-asg',
                          'aws-us-east-2' : 'aws-sct-builders-us-east-2-v4-asg',
                          'aws-eu-west-3' : 'aws-sct-builders-eu-west-3-v4-asg',
                          'aws-ca-central-1' : 'aws-sct-builders-ca-central-1-v4-asg',
                          'gce-us-east1': "${gcp_project}-builders-us-east1-template-v9",
                          'gce-us-east4': "${gcp_project}-builders-us-east4-template-v9",
                          'gce-us-west1': "${gcp_project}-builders-us-west1-template-v9",
                          'gce-us-central1': "${gcp_project}-builders-us-central1-template-v9",
                          'gce': "${gcp_project}-builders-us-east1-template-v9",
                          'aws': 'aws-sct-builders-eu-west-1-v4-asg',
                          'azure-eastus': 'aws-sct-builders-us-east-1-v4-asg',
                          'aws-fips': 'aws-sct-builders-us-east-1-v4-fibs-CI-FIPS',
                          'oci': 'oci-sct-builders-us-phoenix-1-v2',
                          'oci-us-ashburn-1': 'oci-sct-builders-us-ashburn-1-v2',
                          'oci-us-phoenix-1': 'oci-sct-builders-us-phoenix-1-v2',
                          'oci-eu-frankfurt-1': 'oci-sct-builders-eu-frankfurt-1-v2',
                          'minicloud': 'minicloud-kvm-builders-v1',
                          ]

    def cloud_provider = getCloudProviderFromBackend(backend)

    // for xcloud backend, use the underlying cloud provider
    if (backend == 'xcloud') {
        cloud_provider = params.xcloud_provider?.trim()?.toLowerCase()
    }

    def supported_regions_by_provider = [
        'aws': ['eu-west-2', 'eu-north-1', 'eu-central-1', 'us-west-2', 'us-east-2', 'eu-west-3', 'ca-central-1'],
        'gce': ['us-east1', 'us-east4', 'us-west1', 'us-central1'],
        'azure': ['eastus'],
        'oci': ['us-ashburn-1', 'us-phoenix-1', 'eu-frankfurt-1'],
    ]

    // The effective backend region, shared by every return below: GCE/Azure/OCI callers pass
    // their region in datacenter/location/oci_region while `region` stays null. The pipelines
    // hand the returned value straight to createSctRunner and the collect/clean stages, which
    // act on the real cloud with it, so it must be a real region for every backend - the label
    // alone is never enough.
    def effectiveRegion = region
    if (cloud_provider == 'gce') {
        effectiveRegion = datacenter
    } else if (cloud_provider == 'azure') {
        effectiveRegion = location
    } else if (cloud_provider == 'oci') {
        effectiveRegion = oci_region
    }

    // Both early returns below deliberately sit after the JSON-list unwrapping (so `region` is a
    // plain string) and before the region->builder mapping: a build that runs on a local agent has
    // no region builder to find, and for minicloud the region is the one it *emulates*.

    // Pin this build to a named Jenkins agent, on any backend. Declaring the `jenkins_label`
    // parameter in a pipeline is the entire integration - this function already reads the global
    // `params` binding. Empty on build #1, where params is not yet populated, so the normal
    // mapping applies there and nothing changes for the ~1100 jobs that never set it.
    def pinnedLabel = params.jenkins_label?.trim()
    if (pinnedLabel) {
        // A single agent label, never a label *expression*: Jenkins would happily evaluate
        // `a||b` here, so a free-form value lets whoever can trigger the job aim it at agents
        // that were never meant to run SCT - with this job's credentials bound.
        if (!(pinnedLabel ==~ /^[\w][\w.\-]*$/)) {
            error("jenkins_label must be a single agent label (letters, digits, dot, dash, " +
                  "underscore), not a label expression - got '${pinnedLabel}'")
        }
        // and only a label this repository approves: everything this function can already map
        // to, the minicloud KVM agent family, or `pinnableExtras`. A machine becomes pinnable
        // by a one-line addition here, so there is a review trail for "this host may run SCT
        // jobs, with their credentials". Individual lab machines go into the extras list.
        def pinnableExtras = []
        def pinnable = new ArrayList(pinnableExtras)
        for (v in jenkins_labels.values()) {
            pinnable.add(v.toString())  // GCE entries are GStrings - normalize before compare
        }
        if (!pinnable.contains(pinnedLabel) && !(pinnedLabel ==~ /^minicloud-kvm-builders-[\w.\-]+$/)) {
            error("jenkins_label '${pinnedLabel}' is not an approved SCT agent label: use a " +
                  "builder label from getJenkinsLabels.groovy, a minicloud-kvm-builders-* label, " +
                  "or add the new label to pinnableExtras in that file")
        }
        if (effectiveRegion == 'random') {
            def choices = new ArrayList(supported_regions_by_provider.get(cloud_provider, ['eu-west-1']))
            Collections.shuffle(choices)
            effectiveRegion = choices[0]
        }
        println("Pinned to Jenkins agent label: " + pinnedLabel + ", region: " + (effectiveRegion ?: 'eu-west-1'))
        return [ "label": pinnedLabel, "region": effectiveRegion ?: 'eu-west-1' ]
    }

    // minicloud boots QEMU/KVM guests on the agent itself, so it needs a KVM-capable node rather
    // than a region builder. Comes through `overrides` rather than `params` because there is no
    // params.minicloud on build #1, which is when the agent label is first evaluated.
    if (overrides.minicloud) {
        if (effectiveRegion == 'random') {
            throw new Exception("=================== minicloud needs a fixed region, not 'random': it " +
                                "validates uncached AMIs against its own --aws-region ===================")
        }
        def minicloudLabel = jenkins_labels['minicloud']
        println("minicloud run: using KVM-capable agent label " + minicloudLabel)
        return [ "label": minicloudLabel, "region": effectiveRegion ?: 'eu-west-1' ]
    }

    if (effectiveRegion && cloud_provider in ['aws', 'gce', 'azure', 'aws-fibs', 'oci']) {
        def supported_regions = supported_regions_by_provider.get(cloud_provider, [])
        region = effectiveRegion

        println("Finding builder for region: " + region)
        if (region == "random") {
            def choices = new ArrayList(supported_regions)
            Collections.shuffle(choices)
            region = choices[0]
        }

        def cp_region = cloud_provider + "-" + region
        println("Checking if we have a label for " + cp_region)

        def label = jenkins_labels.get(cp_region, null)
        if (label != null) {
            println("Found builder with label: " + label)
            return [ "label": label, "region": region ]
        } else {
            throw new Exception("=================== ${cloud_provider} region ${region} not supported ! ===================")
        }
    } else if (region == 'fips') {
        return [ "label": jenkins_labels['aws-fips'], "region": '' ]
    } else {
        def label = jenkins_labels.get(cloud_provider, null)
        if (label == null) {
            throw new Exception("=================== No Jenkins builder label mapping found for backend '${backend}' (resolved " +
                                "to cloud_provider '${cloud_provider}'). Available mappings: ${jenkins_labels.keySet().sort()} ===================")
        }
        return [ "label": label, "region": region ]
    }
}
