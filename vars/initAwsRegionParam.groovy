#!groovy

def String initAwsRegionParam(String regionStr, String builder_region){
    if (regionStr == "random"){
        return builder_region
    }
    else{
        return groovy.json.JsonOutput.toJson(regionStr)
    }
}
