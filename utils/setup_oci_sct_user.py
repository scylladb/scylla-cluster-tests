#!/usr/bin/env python3
"""
Script to create a dedicated OCI user for SCT builders and generate oci.json credentials.

1. Creates a new IAM user named 'oci-sct-builder'
2. Creates and attaches appropriate IAM policies
3. Generates API key pair
4. Outputs oci.json file with all credentials embedded

Prerequisites:
- OCI Python SDK installed: pip install oci
- OCI CLI configured with admin privileges (~/.oci/config)
- Python 3.8+

Usage:
    python scripts/setup_oci_sct_user.py --compartment-ocid <OCID> --region <REGION>

Example:
    python scripts/setup_oci_sct_user.py \\
        --compartment-ocid ocid1.compartment.oc1..aaaaaaaXXXXX \\
        --region us-phoenix-1 \\
        --output-file oci.json
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

try:
    import oci
    from oci.identity import IdentityClient
    from oci.identity.models import (
        AddUserToGroupDetails,
        CreateApiKeyDetails,
        CreateGroupDetails,
        CreatePolicyDetails,
        CreateUserDetails,
        UpdatePolicyDetails,
    )
except ImportError:
    print("ERROR: OCI Python SDK not installed.")
    print("Install it with: pip install oci")
    sys.exit(1)

# Import RSA from cryptography for key generation
try:
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
except ImportError:
    print("ERROR: cryptography library not installed.")
    print("Install it with: pip install cryptography")
    sys.exit(1)


def load_oci_config(config_file: str = None, profile: str = "DEFAULT") -> dict:
    """Load OCI configuration from config file.

    Args:
        config_file: Path to OCI config file (default: ~/.oci/config)
        profile: Profile name to use from config file

    Returns:
        OCI configuration dict
    """
    if config_file:
        config = oci.config.from_file(file_location=config_file, profile_name=profile)
    else:
        config = oci.config.from_file(profile_name=profile)

    # Validate config
    oci.config.validate_config(config)
    return config


def generate_rsa_key_pair():
    """Generate RSA key pair for OCI API authentication.

    Returns:
        tuple: (private_key_pem, public_key_pem) as strings
    """
    print("Generating RSA key pair (2048-bit)...")

    # Generate private key
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048, backend=default_backend())

    # Serialize private key to PEM format
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")

    # Generate public key
    public_key = private_key.public_key()

    # Serialize public key to PEM format
    public_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM, format=serialization.PublicFormat.SubjectPublicKeyInfo
    ).decode("utf-8")

    return private_pem, public_pem


def create_sct_user(
    identity_client: IdentityClient, compartment_id: str, user_name: str = "oci-sct-builder", email: str = None
) -> dict:
    """Create IAM user for SCT builders.

    Args:
        identity_client: OCI Identity client
        compartment_id: Root compartment (tenancy) OCID
        user_name: Name for the user
        email: Email address for the user (required by OCI)

    Returns:
        User object
    """
    print(f"\nCreating IAM user '{user_name}'...")

    # Check if user already exists
    try:
        users = identity_client.list_users(compartment_id=compartment_id).data
        for user in users:
            if user.name == user_name:
                print(f"User '{user_name}' already exists (OCID: {user.id})")
                response = input("Do you want to use this existing user? [y/N]: ")
                if response.lower() == "y":
                    return user
                else:
                    print("Exiting. Please choose a different user name or delete the existing user.")
                    sys.exit(1)
    except oci.exceptions.ServiceError as e:
        print(f"Warning: Could not check for existing users: {e}")

    # If email not provided, prompt for it
    if not email:
        email = "no-reply@scylladb.com"

    # Create user
    user_details = CreateUserDetails(
        compartment_id=compartment_id,
        name=user_name,
        description="Service user for SCT builders - created by setup_oci_sct_user.py",
        email=email,
    )

    try:
        user = identity_client.create_user(user_details).data
        print("  User created successfully")
        print(f"  User OCID: {user.id}")
        print(f"  User Name: {user.name}")
        return user
    except oci.exceptions.ServiceError as e:
        print(f"ERROR: Failed to create user: {e}")
        sys.exit(1)


def create_sct_group(
    identity_client: IdentityClient, compartment_id: str, group_name: str = "oci-sct-builders"
) -> dict:
    """Create IAM group for SCT builders.

    Args:
        identity_client: OCI Identity client
        compartment_id: Root compartment (tenancy) OCID
        group_name: Name for the group

    Returns:
        Group object
    """
    print(f"\nCreating IAM group '{group_name}'...")

    # Check if group already exists
    try:
        groups = identity_client.list_groups(compartment_id=compartment_id).data
        for group in groups:
            if group.name == group_name:
                print(f"Group '{group_name}' already exists (OCID: {group.id})")
                return group
    except oci.exceptions.ServiceError as e:
        print(f"Warning: Could not check for existing groups: {e}")

    # Create group
    group_details = CreateGroupDetails(
        compartment_id=compartment_id,
        name=group_name,
        description="Group for SCT builders - created by setup_oci_sct_user.py",
    )

    try:
        group = identity_client.create_group(group_details).data
        print("  Group created successfully")
        print(f"  Group OCID: {group.id}")
        print(f"  Group Name: {group.name}")
        return group
    except oci.exceptions.ServiceError as e:
        print(f"ERROR: Failed to create group: {e}")
        sys.exit(1)


def add_user_to_group(identity_client: IdentityClient, user_id: str, group_id: str, user_name: str) -> bool:
    """Add user to group.

    Args:
        identity_client: OCI Identity client
        user_id: User OCID
        group_id: Group OCID
        user_name: User name (for display)

    Returns:
        True if successful, False otherwise
    """
    print(f"\nAdding user '{user_name}' to group...")

    # Check if user is already in group by listing group members
    try:
        group_members = identity_client.list_user_group_memberships(user_id=user_id).data
        for member in group_members:
            if member.group_id == group_id:
                print("  User is already a member of the group")
                return True
    except oci.exceptions.ServiceError as e:
        print(f"Warning: Could not check existing group memberships: {e}")

    # Add user to group
    add_user_details = AddUserToGroupDetails(user_id=user_id, group_id=group_id)

    try:
        identity_client.add_user_to_group(add_user_details)
        print("  User added to group successfully")
        return True
    except oci.exceptions.ServiceError as e:
        # Check if the error is because user is already in group
        error_str = str(e).lower()
        if "duplicate" in error_str or "already" in error_str:
            print("  User is already a member of the group")
            return True

        print(f"ERROR: Failed to add user to group: {e}")
        return False


def create_sct_policy(  # noqa: PLR0915
    identity_client: IdentityClient, compartment_id: str, group_name: str, policy_compartment_id: str
) -> dict | None:
    """Create IAM policy for SCT group.

    Args:
        identity_client: OCI Identity client
        compartment_id: Root compartment (tenancy) OCID for policy creation
        group_name: Name of the SCT group
        policy_compartment_id: Compartment OCID where builders will run

    Returns:
        Policy object or None if creation failed
    """
    # Clean up group name - remove any extra whitespace
    group_name = str(group_name).strip()

    print(f"\nCreating IAM policy for group '{group_name}'...")

    # Validate group name - OCI is strict about what's allowed in policy statements
    if not group_name or not isinstance(group_name, str):
        print(f"ERROR: Invalid group name: {repr(group_name)}")
        return None

    if any(c in group_name for c in ['"', "'", "\n", "\r", "\t"]):
        print(f"ERROR: Group name contains invalid characters: {repr(group_name)}")
        return None

    # Check for spaces in group name - if present, we need to use quotes
    # However, OCI group names typically don't have spaces, so this is just defensive
    if " " in group_name:
        group_ref = f'"{group_name}"'
    else:
        group_ref = group_name

    policy_name = "oci-sct-builder-policy"

    # Check if policy already exists
    existing_policy = None
    try:
        policies = identity_client.list_policies(compartment_id=compartment_id).data
        for policy in policies:
            if policy.name == policy_name:
                existing_policy = policy
                print(f"Policy '{policy_name}' already exists (OCID: {policy.id})")
                print("Will update with new statements...")
                break
    except oci.exceptions.ServiceError as e:
        print(f"Warning: Could not check for existing policies: {e}")

    # Compartment-level policies - minimal working set
    policy_statements = [
        f"allow group {group_ref} to manage instance in compartment id {compartment_id}",
        f"allow group {group_ref} to manage instance-images in compartment id {compartment_id}",
        f"allow group {group_ref} to manage volume in compartment id {compartment_id}",
        f"allow group {group_ref} to manage volume-attachments in compartment id {compartment_id}",
        f"allow group {group_ref} to manage boot-volumes in compartment id {compartment_id}",
        f"allow group {group_ref} to manage boot-volume-attachments in compartment id {compartment_id}",
        f"allow group {group_ref} to manage virtual-network-family in compartment id {compartment_id}",
        f"allow group {group_ref} to manage vcn in compartment id {compartment_id}",
        f"allow group {group_ref} to manage subnet in compartment id {compartment_id}",
        f"allow group {group_ref} to manage internet-gateway in compartment id {compartment_id}",
        f"allow group {group_ref} to manage security-list in compartment id {compartment_id}",
        f"allow group {group_ref} to manage route-table in compartment id {compartment_id}",
        f"allow group {group_ref} to manage dhcp-options in compartment id {compartment_id}",
        f"allow group {group_ref} to read app-catalog-listing in compartment id {compartment_id}",
    ]

    # Debug: print what we're about to send
    print("\nDebug - Policy details to send:")
    print(f"  Group Name (as used in policy): {repr(group_ref)}")
    print(f"  Compartment ID: {compartment_id}")
    print(f"  Policy Name: {policy_name}")
    print(f"  Statements count: {len(policy_statements)}")
    for i, stmt in enumerate(policy_statements, 1):
        print(f"    {i}. {repr(stmt)}")

    # Create or update policy with statements
    try:
        # Ensure statements are clean strings
        clean_statements = [str(s).strip() for s in policy_statements]

        print(
            f"\nDebug - Attempting to {'update' if existing_policy else 'create'} policy with {len(clean_statements)} statement(s)..."
        )
        print("  Statements being sent:")
        for i, stmt in enumerate(clean_statements, 1):
            print(f"    [{i}] {repr(stmt)}")

        if existing_policy:
            # Update existing policy
            print("\n  Updating existing policy...")
            policy_update_details = UpdatePolicyDetails(
                description="Policy for SCT builders - created by setup_oci_sct_user.py", statements=clean_statements
            )

            policy = identity_client.update_policy(
                policy_id=existing_policy.id, update_policy_details=policy_update_details
            ).data
            print("  Policy updated successfully")
        else:
            # Create new policy
            print("\n  Creating new policy...")
            policy_details = CreatePolicyDetails(
                compartment_id=compartment_id,
                name=policy_name,
                description="Policy for SCT builders - created by setup_oci_sct_user.py",
                statements=clean_statements,
            )

            policy = identity_client.create_policy(policy_details).data
            print("  Policy created successfully")

        print(f"  Policy OCID: {policy.id}")
        print(f"  Policy Name: {policy.name}")
        print(f"  Statements: {len(clean_statements)} permissions granted")
        return policy
    except oci.exceptions.ServiceError as e:
        # Try fallback with simpler all-resources permission
        print(
            f"\n⚠ Initial policy {'update' if existing_policy else 'creation'} failed, attempting fallback with all-resources..."
        )

        error_str = str(e).lower()
        if "no permissions found" in error_str or "invalidparameter" in error_str:
            try:
                fallback_statements = [
                    f"allow group {group_ref} to manage all-resources in compartment id {compartment_id}"
                ]

                print("\n  Retrying with fallback statement:")
                print(f"    {repr(fallback_statements[0])}")

                if existing_policy:
                    # Update with fallback statement
                    policy_update_details = UpdatePolicyDetails(
                        description="Policy for SCT builders - created by setup_oci_sct_user.py (all-resources fallback)",
                        statements=fallback_statements,
                    )
                    policy = identity_client.update_policy(
                        policy_id=existing_policy.id, update_policy_details=policy_update_details
                    ).data
                else:
                    # Create with fallback statement
                    policy_details = CreatePolicyDetails(
                        compartment_id=compartment_id,
                        name=policy_name,
                        description="Policy for SCT builders - created by setup_oci_sct_user.py (all-resources fallback)",
                        statements=fallback_statements,
                    )
                    policy = identity_client.create_policy(policy_details).data

                print(f"  Policy {'updated' if existing_policy else 'created'} successfully (with fallback statement)")
                print(f"  Policy OCID: {policy.id}")
                print(f"  Policy Name: {policy.name}")
                return policy
            except oci.exceptions.ServiceError as fallback_error:
                print(f"  Fallback also failed: {fallback_error}")

        # If we get here, both attempts failed
        clean_statements = [str(s).strip() for s in policy_statements]

        print(f"\n  ERROR: Failed to {'update' if existing_policy else 'create'} policy (both attempts)")
        print(f"  Error type: {type(e).__name__}")
        print(f"  Error message: {e}")
        print("\nDebug info - Policy statements that failed:")
        for i, stmt in enumerate(clean_statements, 1):
            print(f"  [{i}] {repr(stmt)}")
        print("\nRequest details:")
        print(f"  Policy ID: {repr(existing_policy.id if existing_policy else 'N/A (new policy)')}")
        print(f"  Compartment ID: {repr(compartment_id)}")
        print(f"  Policy Name: {repr(policy_name)}")
        print(f"  Statements count: {len(clean_statements)}")

        print("\n" + "=" * 70)
        print("POLICY {'UPDATE' if existing_policy else 'CREATION'} FAILED - MANUAL UPDATE REQUIRED")
        print("=" * 70)
        print("\nTo update the policy manually, use the OCI Console or CLI:")
        print("\nUsing OCI CLI:")
        if existing_policy:
            print("  oci iam policy update \\")
            print(f"    --policy-id {existing_policy.id} \\")
        else:
            print("  oci iam policy create \\")
            print(f"    --compartment-id {compartment_id} \\")
        print(f"    --name {policy_name} \\")
        print("    --description 'Policy for SCT builders - created manually' \\")
        print(f"    --statements '[\"allow group {group_ref} to manage all-resources in tenancy\"]'")

        print("\nOr using OCI Console:")
        print("  1. Go to Identity & Security > Policies")
        if existing_policy:
            print(f"  2. Find and select policy: {policy_name}")
            print("  3. Click Edit")
        else:
            print("  2. Create Policy")
            print(f"  3. Name: {policy_name}")
        print("  4. Add this statement:")

        print("\n" + "=" * 70)
        # Don't exit - user can continue with manual policy creation or update
        return None


def get_existing_api_keys(identity_client: IdentityClient, user_id: str) -> list:
    """Get existing API keys for a user.

    Args:
        identity_client: OCI Identity client
        user_id: User OCID

    Returns:
        List of existing API key objects
    """
    try:
        api_keys = identity_client.list_api_keys(user_id).data
        return api_keys if api_keys else []
    except oci.exceptions.ServiceError as e:
        print(f"Warning: Could not retrieve existing API keys: {e}")
        return []


def upload_api_key(identity_client: IdentityClient, user_id: str, public_key_pem: str) -> tuple:
    """Upload API public key to user.

    Skips upload if user already has API keys attached.

    Args:
        identity_client: OCI Identity client
        user_id: User OCID
        public_key_pem: Public key in PEM format

    Returns:
        tuple: (api_key_object, fingerprint) or (None, None) if keys already exist
    """
    # Check for existing keys
    existing_keys = get_existing_api_keys(identity_client, user_id)

    if existing_keys:
        print(f"\n⚠ User already has {len(existing_keys)} API key(s) attached:")
        for i, key in enumerate(existing_keys, 1):
            print(f"  {i}. Fingerprint: {key.fingerprint}")
            print(f"     Status: {key.lifecycle_state}")
            print(f"     Created: {key.time_created}")
        print("Skipping API key upload.")

        # Use the first active key for credentials
        for key in existing_keys:
            if key.lifecycle_state == "ACTIVE":
                return key, key.fingerprint

        # If no active key found, return the first key anyway
        return existing_keys[0], existing_keys[0].fingerprint

    print("\nUploading API public key to user...")

    api_key_details = CreateApiKeyDetails(key=public_key_pem)

    try:
        api_key = identity_client.upload_api_key(user_id, api_key_details).data
        print("  API key uploaded successfully")
        print(f"  Fingerprint: {api_key.fingerprint}")
        return api_key, api_key.fingerprint
    except oci.exceptions.ServiceError as e:
        print(f"ERROR: Failed to upload API key: {e}")
        sys.exit(1)


def save_oci_json(oci_config: dict, output_file: str):
    """Save OCI configuration to JSON file.

    Args:
        oci_config: OCI configuration dictionary
        output_file: Output file path
    """
    output_path = Path(output_file)

    # Check if file exists
    if output_path.exists():
        response = input(f"\nFile '{output_file}' already exists. Overwrite? [y/N]: ")
        if response.lower() != "y":
            print("Aborting. No files were modified.")
            sys.exit(1)

    # Create backup if file exists
    if output_path.exists():
        backup_file = f"{output_file}.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        print(f"Creating backup: {backup_file}")
        output_path.rename(backup_file)

    # Write JSON file
    with open(output_file, "w") as f:
        json.dump(oci_config, f, indent=2)

    print(f"\n  OCI credentials saved to: {output_file}")
    print(f"\n{'=' * 70}")
    print("IMPORTANT: This file contains sensitive credentials!")
    print("- Do NOT commit it to version control")
    print("- Store it securely (e.g., upload to S3 keystore)")
    print("- Set appropriate file permissions: chmod 600")
    print(f"{'=' * 70}")


def save_local_oci_config(
    tenancy_id: str,
    user_id: str,
    fingerprint: str,
    region: str,
    compartment_id: str,
    key_file_path: str,
    config_file: str = "~/.oci/config",
):
    """Save OCI configuration to local config file format.

    Args:
        tenancy_id: Tenancy OCID
        user_id: User OCID
        fingerprint: API key fingerprint
        region: OCI region
        compartment_id: Compartment OCID
        key_file_path: Path to private key file
        config_file: Config file path
    """
    config_path = Path(config_file).expanduser()

    config_content = f"""
[OCI-SCT-BUILDER]
user={user_id}
fingerprint={fingerprint}
key_file={key_file_path}
tenancy={tenancy_id}
region={region}
compartment_id={compartment_id}
"""

    print(f"\n  Optional: Add this configuration to {config_path}:")
    print(config_content)


def main():  # noqa: PLR0914
    parser = argparse.ArgumentParser(
        description="Create OCI SCT user and generate credentials",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument("--compartment-ocid", required=False, help="Compartment OCID where SCT builders will run")

    parser.add_argument("--region", required=True, help="OCI region identifier (e.g., us-phoenix-1)")

    parser.add_argument(
        "--user-name", default="oci-sct-builder", help="Name for the SCT user (default: oci-sct-builder)"
    )

    parser.add_argument("--email", help="Email address for the SCT user (required by OCI, will prompt if not provided)")

    parser.add_argument("--output-file", default="oci.json", help="Output file path for oci.json (default: oci.json)")

    parser.add_argument("--config-file", help="Path to OCI config file (default: ~/.oci/config)")

    parser.add_argument("--profile", default="DEFAULT", help="OCI config profile to use (default: DEFAULT)")

    parser.add_argument("--save-keys", action="store_true", help="Save private/public key files to ~/.oci/")

    args = parser.parse_args()

    print("=" * 70)
    print("OCI SCT User Setup Script")
    print("=" * 70)

    # Load OCI configuration
    print(f"\nLoading OCI configuration (profile: {args.profile})...")
    try:
        config = load_oci_config(args.config_file, args.profile)
        print("  Configuration loaded")
        print(f"  Tenancy: {config['tenancy']}")
        print(f"  Region: {config['region']}")
    except (oci.exceptions.ConfigFileNotFound, oci.exceptions.InvalidConfig, KeyError) as e:
        print(f"ERROR: Failed to load OCI configuration: {e}")
        print("\nMake sure you have:")
        print("1. OCI CLI configured (~/.oci/config)")
        print("2. Valid credentials with admin privileges")
        sys.exit(1)

    # Initialize Identity client
    identity_client = IdentityClient(config)
    tenancy_id = config["tenancy"]

    # Get compartment OCID from args or config
    compartment_ocid = args.compartment_ocid
    if not compartment_ocid:
        # Try to read from config file (supports both 'compartment_id' and 'compartment_ocid')
        compartment_ocid = config.get("compartment_id") or config.get("compartment_ocid")
        if not compartment_ocid:
            # Default to tenancy OCID (root compartment)
            compartment_ocid = tenancy_id
            print(f"Using tenancy OCID as compartment (root compartment): {compartment_ocid}")
        else:
            print(f"Using compartment OCID from config: {compartment_ocid}")
    else:
        print(f"Using compartment OCID from argument: {compartment_ocid}")

    # Step 1: Create user
    user = create_sct_user(identity_client, tenancy_id, args.user_name, args.email)

    # Step 2: Create group
    group = create_sct_group(identity_client, tenancy_id)

    # Step 3: Add user to group
    add_user_to_group(identity_client, user.id, group.id, args.user_name)

    # Step 4: Create policy for the group
    policy = create_sct_policy(identity_client, tenancy_id, group.name, compartment_ocid)

    # Step 5: Generate API key pair
    private_key_pem, public_key_pem = generate_rsa_key_pair()

    # Optional: Save keys to files
    if args.save_keys:
        oci_dir = Path.home() / ".oci"
        oci_dir.mkdir(exist_ok=True)

        private_key_file = oci_dir / f"{args.user_name}_api_key.pem"
        public_key_file = oci_dir / f"{args.user_name}_api_key_public.pem"

        with open(private_key_file, "w") as f:
            f.write(private_key_pem)
        private_key_file.chmod(0o600)

        with open(public_key_file, "w") as f:
            f.write(public_key_pem)
        public_key_file.chmod(0o644)

        print("\n  Keys saved:")
        print(f"  Private key: {private_key_file}")
        print(f"  Public key: {public_key_file}")

    # Step 6: Upload public key to user
    api_key, fingerprint = upload_api_key(identity_client, user.id, public_key_pem)

    # Step 7: Create oci.json
    oci_config = {
        "tenancy": tenancy_id,
        "user": user.id,
        "fingerprint": fingerprint,
        "key_content": private_key_pem,
        "region": args.region,
        "compartment_id": compartment_ocid,
    }

    # Step 8: Save oci.json
    save_oci_json(oci_config, args.output_file)

    # Step 9: Display local config format (optional)
    if args.save_keys:
        save_local_oci_config(
            tenancy_id=tenancy_id,
            user_id=user.id,
            fingerprint=fingerprint,
            region=args.region,
            compartment_id=compartment_ocid,
            key_file_path=str(Path.home() / ".oci" / f"{args.user_name}_api_key.pem"),
        )

    # Summary
    print(f"\n{'=' * 70}")
    print("  Setup Complete!")
    print(f"{'=' * 70}")
    print("\nCreated resources:")
    print(f"  User OCID: {user.id}")
    print(f"  User Name: {user.name}")
    print(f"  Group OCID: {group.id}")
    print(f"  Group Name: {group.name}")
    if policy:
        print(f"  Policy OCID: {policy.id}")
    print(f"  API Key Fingerprint: {fingerprint}")
    print(f"\nCredentials file: {args.output_file}")
    print("\nNext steps:")
    print(f"1. Upload to S3 keystore: aws s3 cp {args.output_file} s3://scylla-qa-keystore/oci.json")
    print(f"2. Verify credentials work: oci iam user get --user-id {user.id}")
    print(f"3. Set file permissions: chmod 600 {args.output_file}")


if __name__ == "__main__":
    main()
