#!/bin/bash

# Initialize the base command
cmd="mcp install"

# Read requirements.in and process each line
while IFS= read -r line || [ -n "$line" ]; do
    # Skip empty lines and comments
    [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue

    # Get the full package specification, but remove any trailing comments
    package_spec=$(echo "$line" | sed -E 's/[[:space:]]*#.*$//' | tr -d ' ')

    # Skip empty lines after comment removal
    [[ -z "$package_spec" ]] && continue

    # Only include specific packages
    if [[ "$package_spec" =~ ^(boto3|boto3-stubs|requests|paramiko) ]]; then
        # Append --with flag with quoted package specification
        cmd="$cmd --with $package_spec"
    fi
done < requirements.in

cmd="$cmd --env-var UV_PYTHON=3.10 mcp_server.py"

# Execute the command
echo "Executing: $cmd"
exec $cmd
