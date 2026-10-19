# Refresh (sstable loading) tests

[← All configuration options](../configuration_options.md)

Loading pre-built SSTables into a running cluster via nodetool refresh.

**3 options.**


<a id="sstable_file"></a>

## **sstable_file** / SCT_SSTABLE_FILE

Local path of the SSTable archive to load with 'nodetool refresh'.

**default:** N/A

**type:** str (appendable)


<a id="sstable_md5"></a>

## **sstable_md5** / SCT_SSTABLE_MD5

Expected MD5 of the downloaded SSTable archive, used to verify the download.

**default:** N/A

**type:** str (appendable)


<a id="sstable_url"></a>

## **sstable_url** / SCT_SSTABLE_URL

URL the SSTable archive is downloaded from when it is not already on the node.

**default:** N/A

**type:** str (appendable)
