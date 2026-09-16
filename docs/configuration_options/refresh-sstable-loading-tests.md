# Refresh (sstable loading) tests

[← All configuration options](../configuration_options.md)

Loading pre-built SSTables into a running cluster via nodetool refresh.

**6 options.**


<a id="flush_period"></a>

## **flush_period** / SCT_FLUSH_PERIOD

Seconds to wait between the flushes controlled by [`flush_times`](#flush_times).

**default:** N/A

**type:** int


<a id="flush_times"></a>

## **flush_times** / SCT_FLUSH_TIMES

How many times to flush the memtable to disk during the refresh test.

**default:** N/A

**type:** int


<a id="skip_download"></a>

## **skip_download** / SCT_SKIP_DOWNLOAD

Skip downloading the SSTable archive and reuse a copy already on the node.

**default:** False

**type:** bool


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
