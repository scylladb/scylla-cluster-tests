# Create ES index

ES index stores all documents. ES document could have fields with different data type. The mapping of document properties and field types described in es mapping settings. ES could detect the field type dynamically and generate mapping when first document is written to ES index. After mapping is added to index, it could be changed.

Detected type for document properties is not always correct.

Example, Performance tests use test_details.start_time field to store date when test was start executed. But data type was set in mapping as str/long for different indexes. This doesn't allow to build Kibana dashboards based on start_time.

To avoid this issues, ES Index could be create with specified mappings schema before any document added to the index. Any addition settings and formants could be configured for certain field in future documents.

Sct creates ES Index using the Class name of maing test,

```python
class PerformanceTest(ClusterTester):
	pass

class LongevityTest(ClusterTester):
	pass
```

Once test is configured to store results and is run, SCT create new index, if it is not yet exists, with name `performancetest` or `longevitytest`, but field mapping will be done dynamically and by fields of first inserted document.

To be sure that certain document property has required data type, index could be created with mapping settings provided in file with new sct commands

# SCT commands

1. Create file with mappings with any editor you prefer.
2. Run sct to create new index in ES:
```bash
# hydra create-es-index --name <index_name> --doc-type <doc_type_in_index> --filepath <full_path_to_file_with_mapping>
hydra create-es-index --name newtestindex --doc-type test_stats --filepath "configration/es_mappings/newmapping.json"

```
3. sct test automatically will write new document to new index if its class name will `NewTestIndex
-or-
4. you can use ES client to write document to index `newtestindex`
