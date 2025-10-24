#!/bin/bash
# Example usage of the find-ami-equivalent hydra command
#
# This command helps you find equivalent AMIs in different regions or architectures
# based on image tags like Name, scylla_version, and build-id

echo "================================================"
echo "find-ami-equivalent Command Usage Examples"
echo "================================================"
echo ""

echo "1. Show help:"
echo "   ./sct.py find-ami-equivalent --help"
echo ""

echo "2. Find equivalent AMI in the same region (validates tags work):"
echo "   ./sct.py find-ami-equivalent \\"
echo "       --ami-id ami-0d9726c9053daff76 \\"
echo "       --source-region us-east-1"
echo ""

echo "3. Find equivalent AMI in different regions:"
echo "   ./sct.py find-ami-equivalent \\"
echo "       --ami-id ami-0d9726c9053daff76 \\"
echo "       --source-region us-east-1 \\"
echo "       --target-region us-west-2 \\"
echo "       --target-region eu-west-1"
echo ""

echo "4. Find ARM64 equivalent of an x86_64 AMI:"
echo "   ./sct.py find-ami-equivalent \\"
echo "       --ami-id ami-0d9726c9053daff76 \\"
echo "       --source-region us-east-1 \\"
echo "       --target-arch arm64"
echo ""

echo "5. Get JSON output for pipeline use:"
echo "   ./sct.py find-ami-equivalent \\"
echo "       --ami-id ami-0d9726c9053daff76 \\"
echo "       --source-region us-east-1 \\"
echo "       --target-region us-west-2 \\"
echo "       --output-format json"
echo ""

echo "6. Full example with multiple regions and architecture:"
echo "   ./sct.py find-ami-equivalent \\"
echo "       --ami-id ami-0d9726c9053daff76 \\"
echo "       --source-region us-east-1 \\"
echo "       --target-region us-east-1 \\"
echo "       --target-region us-west-2 \\"
echo "       --target-region eu-west-1 \\"
echo "       --target-region eu-central-1 \\"
echo "       --target-arch arm64 \\"
echo "       --output-format table"
echo ""

echo "================================================"
echo "Output Formats:"
echo "================================================"
echo ""
echo "- table: Human-readable table format (default)"
echo "- json:  JSON format for pipeline integration"
echo ""

echo "================================================"
echo "Use Cases:"
echo "================================================"
echo ""
echo "1. Converting AMI ID from one region to another"
echo "2. Finding ARM64 equivalent of x86_64 AMI"
echo "3. Validating AMI availability across regions"
echo "4. Pipeline automation for multi-region deployments"
echo ""

echo "================================================"
echo "Note: Requires AWS credentials configured"
echo "================================================"
