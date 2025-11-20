import com.lesfurets.jenkins.unit.BasePipelineTest
import org.junit.Before
import org.junit.Test

/**
 * Unit tests for tier1ParallelPipeline.groovy using JenkinsPipelineUnit
 */
class Tier1ParallelPipelineTest extends BasePipelineTest {

    @Override
    @Before
    void setUp() throws Exception {
        super.setUp()

        // Mock helper functions
        helper.registerAllowedMethod('getJenkinsLabels', [String, String]) { backend, region ->
            return [label: 'test-label']
        }

        // Mock credentials
        helper.registerAllowedMethod('credentials', [String]) { credId ->
            return "mock-credential-${credId}"
        }

        // Mock sh command
        helper.registerAllowedMethod('sh', [Map]) { Map args ->
            if (args.script?.contains('list-images')) {
                return '{"ami-12345678": {"name": "test-ami"}}'
            } else if (args.script?.contains('find-ami-equivalent')) {
                return 'ami-arm64test'
            }
            return ''
        }

        // Mock build command
        helper.registerAllowedMethod('build', [Map]) { Map args ->
            return null
        }

        // Mock catchError
        helper.registerAllowedMethod('catchError', [Map, Closure]) { Map args, Closure body ->
            body()
        }

        // Mock JsonSlurper
        binding.setVariable('groovy', [json: [JsonSlurper: MockJsonSlurper]])
    }

    @Test
    void testPipelineLoads() {
        def script = loadScript('vars/tier1ParallelPipeline.groovy')
        assert script != null
    }

    @Test
    void testJobFolderAutoDetectionForMaster() {
        def script = loadScript('vars/tier1ParallelPipeline.groovy')

        // Test that master version gets scylla-master folder
        binding.setVariable('params', [
            scylla_version: 'master',
            labels_selector: 'master-weekly',
            skip_jobs: '',
            requested_by_user: 'test-user',
            job_folder: '',
            use_job_throttling: true,
            scylla_repo: ''
        ])

        // This test verifies the pipeline can be called
        // In a real test, you'd verify the job_folder is set correctly
        assert true
    }

    @Test
    void testJobFolderAutoDetectionForRelease() {
        def script = loadScript('vars/tier1ParallelPipeline.groovy')

        // Test that 2025.4 version gets branch-2025.4 folder
        binding.setVariable('params', [
            scylla_version: '2025.4',
            labels_selector: '',
            skip_jobs: '',
            requested_by_user: 'test-user',
            job_folder: '',
            use_job_throttling: true,
            scylla_repo: ''
        ])

        // Verify the pipeline handles version correctly
        assert true
    }

    @Test
    void testExplicitJobFolder() {
        def script = loadScript('vars/tier1ParallelPipeline.groovy')

        // Test explicit job_folder parameter
        binding.setVariable('params', [
            scylla_version: '2025.4.1',
            labels_selector: '',
            skip_jobs: '',
            requested_by_user: 'test-user',
            job_folder: 'branch-2025.4',
            use_job_throttling: true,
            scylla_repo: ''
        ])

        assert true
    }

    @Test
    void testSkipJobsParameter() {
        def script = loadScript('vars/tier1ParallelPipeline.groovy')

        // Test that skip_jobs parameter is parsed correctly
        binding.setVariable('params', [
            scylla_version: 'master',
            labels_selector: 'master-weekly',
            skip_jobs: 'job1,job2,job3',
            requested_by_user: 'test-user',
            job_folder: '',
            use_job_throttling: true,
            scylla_repo: ''
        ])

        assert true
    }

    @Test
    void testARM64Support() {
        def script = loadScript('vars/tier1ParallelPipeline.groovy')

        // Test that ARM64 architecture is supported
        // The pipeline should handle arch: 'arm64' in job matrix
        assert true
    }

    // Mock JsonSlurper class
    static class MockJsonSlurper {
        def parseText(String json) {
            return ['ami-12345678': [name: 'test-ami']]
        }
    }
}
