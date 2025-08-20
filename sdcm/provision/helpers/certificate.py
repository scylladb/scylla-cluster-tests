# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2021 ScyllaDB
from __future__ import annotations

import io
import ipaddress
import logging
import shutil
import tarfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent
from typing import Any, TYPE_CHECKING

from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization.pkcs12 import serialize_key_and_certificates
from cryptography import x509
from cryptography.x509.oid import NameOID

from sdcm.remote import shell_script_cmd
from sdcm.utils.common import get_data_dir_path
from sdcm.utils.docker_utils import ContainerManager, DockerException

if TYPE_CHECKING:
    from sdcm.cluster import BaseNode


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class TLSAssets:
    CA_CERT: str = 'ca.pem'
    CA_KEY: str = 'ca.key'
    DB_CERT: str = 'db.crt'
    DB_KEY: str = 'db.key'
    DB_CSR: str = 'db.csr'
    DB_CLIENT_FACING_CERT: str = 'client-facing.crt'
    DB_CLIENT_FACING_KEY: str = 'client-facing.key'
    CLIENT_CERT: str = 'test.crt'
    CLIENT_KEY: str = 'test.key'
    JKS_TRUSTSTORE: str = 'truststore.jks'
    PKCS12_KEYSTORE: str = 'keystore.p12'


CA_DEFAULT_PASSWORD = 'scylladb'

SCYLLA_SSL_CONF_DIR = Path('/etc/scylla/ssl_conf')
CA_CERT_FILE = Path(get_data_dir_path('ssl_conf', TLSAssets.CA_CERT))
CA_KEY_FILE = Path(get_data_dir_path('ssl_conf', TLSAssets.CA_KEY))

# Cluster artifacts
SERVER_KEY_FILE = Path(get_data_dir_path('ssl_conf', TLSAssets.DB_KEY))
SERVER_CERT_FILE = Path(get_data_dir_path('ssl_conf', TLSAssets.DB_CERT))
CLIENT_FACING_KEYFILE = Path(get_data_dir_path('ssl_conf', TLSAssets.DB_CLIENT_FACING_KEY))
CLIENT_FACING_CERTFILE = Path(get_data_dir_path('ssl_conf', TLSAssets.DB_CLIENT_FACING_CERT))

# Client artifacts
CLIENT_KEY_FILE = Path(get_data_dir_path('ssl_conf', TLSAssets.CLIENT_KEY))
CLIENT_CERT_FILE = Path(get_data_dir_path('ssl_conf', TLSAssets.CLIENT_CERT))
JKS_TRUSTSTORE_FILE = Path(get_data_dir_path('ssl_conf', TLSAssets.JKS_TRUSTSTORE))


def install_client_certificate(remoter, node_identifier):
    if remoter.run(f'ls {SCYLLA_SSL_CONF_DIR}', ignore_status=True).ok:
        return
    dst = '/tmp/ssl_conf'
    remoter.run(f'mkdir -p {dst}')
    remoter.send_files(src=str(Path(get_data_dir_path('ssl_conf')) / node_identifier) + '/', dst=dst)
    remoter.send_files(src=str(Path(get_data_dir_path('ssl_conf')) / 'client'), dst=dst)
    setup_script = dedent(f"""
        mkdir -p ~/.cassandra/
        cp /tmp/ssl_conf/client/cqlshrc ~/.cassandra/
        sed -i '/ssl = true/a hostname = {node_identifier}' ~/.cassandra/cqlshrc
        sudo mkdir -p /root/.cassandra
        sudo cp ~/.cassandra/cqlshrc /root/.cassandra
        sudo mkdir -p /etc/scylla/
        sudo rm -rf {SCYLLA_SSL_CONF_DIR}
        sudo mv -f /tmp/ssl_conf/ /etc/scylla/
    """)
    remoter.run('bash -cxe "%s"' % setup_script)


def install_encryption_at_rest_files(remoter):
    if remoter.sudo('ls /etc/encrypt_conf/system_key_dir', ignore_status=True).ok:
        return
    remoter.send_files(src=get_data_dir_path('encrypt_conf'), dst="/tmp/")

    # Use scylla-test if scylla user doesnt exists
    scylla_user = "scylla:scylla" if remoter.sudo('id scylla', ignore_status=True).ok else "scylla-test:scylla-test"

    remoter.sudo(shell_script_cmd(dedent(f"""
        rm -rf /etc/encrypt_conf
        mv -f /tmp/encrypt_conf /etc
        mkdir -p /etc/scylla/encrypt_conf /etc/encrypt_conf/system_key_dir
        chown -R {scylla_user} /etc/scylla /etc/encrypt_conf
    """)))
    remoter.sudo("md5sum /etc/encrypt_conf/*.pem", ignore_status=True)


def _create_ca(cname: str = 'scylladb.com', valid_days: int = 365) -> None:
    """Generate and save a key and certificate for CA."""
    if CA_CERT_FILE.exists() and CA_KEY_FILE.exists():
        LOGGER.info(
            "CA %s certificate and %s key already exist. Skipping creation.", CA_CERT_FILE, CA_KEY_FILE)
        return

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=4096)
    subject = issuer = x509.Name(
        [
            x509.NameAttribute(NameOID.COUNTRY_NAME, 'US'),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, 'California'),
            x509.NameAttribute(NameOID.LOCALITY_NAME, 'Mountain View'),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, 'ScyllaDB'),
            x509.NameAttribute(NameOID.COMMON_NAME, cname),
        ]
    )
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.utcnow())
        .not_valid_after(datetime.utcnow() + timedelta(days=valid_days))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(private_key.public_key()), critical=False)
        .sign(private_key, hashes.SHA256())
    )

    with open(file=CA_CERT_FILE, mode='wb') as file:
        file.write(cert.public_bytes(serialization.Encoding.PEM))
    with open(file=CA_KEY_FILE, mode='wb') as file:
        file.write(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.BestAvailableEncryption(CA_DEFAULT_PASSWORD.encode())
            )
        )


def create_certificate(
        cert_file: Path, key_file: Path, cname: str, ca_cert_file: Path = None, ca_key_file: Path = None,
        server_csr_file: Path = None, ip_addresses: list = None, dns_names: list = None, valid_days: int = 365) -> None:
    """
    Generate/save CSR, certificate and key.

    Certificate is immediately signed by the CA, so CSR is not used. But it is needed later in some test scenarios
    that update the certificate using the CSR.
    """
    private_key = sign_key = rsa.generate_private_key(public_exponent=65537, key_size=4096)
    subject = issuer = x509.Name(
        [
            x509.NameAttribute(NameOID.COUNTRY_NAME, 'IL'),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, 'Tel Aviv'),
            x509.NameAttribute(NameOID.LOCALITY_NAME, 'Herzelia'),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, 'ScyllaDB'),
            x509.NameAttribute(NameOID.COMMON_NAME, cname),
        ]
    )

    alt_names = [x509.DNSName(cname)]
    if ip_addresses:
        alt_names.extend([x509.IPAddress(ipaddress.ip_address(ip)) for ip in ip_addresses if ip])
    if dns_names:
        alt_names.extend([x509.DNSName(dns) for dns in dns_names if dns])

    if ca_cert_file and ca_key_file:
        with open(ca_cert_file, 'rb') as file:
            ca_cert = x509.load_pem_x509_certificate(file.read())
            issuer = ca_cert.subject
        with open(ca_key_file, 'rb') as file:
            sign_key = serialization.load_pem_private_key(file.read(), password=CA_DEFAULT_PASSWORD.encode())

    if server_csr_file:
        # Create and save CSR for the cert
        server_csr = (
            x509.CertificateSigningRequestBuilder()
            .subject_name(subject)
            .add_extension(x509.SubjectAlternativeName(alt_names), critical=False)
            .sign(private_key, hashes.SHA256())
        )
        with open(server_csr_file, 'wb') as csr_file:
            csr_file.write(server_csr.public_bytes(serialization.Encoding.PEM))

    # Building the certificate
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.utcnow())
        .not_valid_after(datetime.utcnow() + timedelta(days=valid_days))
        .add_extension(x509.SubjectAlternativeName(alt_names), critical=False)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(private_key.public_key()), critical=False)
        .sign(sign_key, hashes.SHA256())
    )

    with open(file=cert_file, mode='wb') as file:
        file.write(cert.public_bytes(serialization.Encoding.PEM))
    with open(file=key_file, mode='wb') as file:
        file.write(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption())
        )


def _import_ca_to_jks_truststore(
        localhost, cert_file: Path = CA_CERT_FILE, jks_file: Path = JKS_TRUSTSTORE_FILE,
        jks_password: str = 'cassandra') -> None:
    """Convert a PEM formatted certificate to a Java truststore."""
    if jks_file.exists():
        LOGGER.info("%s Java truststore already exists. Skipping creation.", jks_file)
        return

    with open(cert_file, encoding="utf-8") as file:
        cert = file.read()

    java = ContainerManager.run_container(localhost, 'java')

    # Create tar archive with CA and put it to container
    tar_stream = io.BytesIO()
    with tarfile.open(fileobj=tar_stream, mode='w') as tar:
        tarinfo = tarfile.TarInfo(name=TLSAssets.CA_CERT)
        tarinfo.size = len(cert)
        tar.addfile(tarinfo, io.BytesIO(cert.encode('utf-8')))
    tar_stream.seek(0)
    java.put_archive('/tmp', tar_stream)

    # Import CA to Java truststore
    tmp_cert_path = Path('/tmp') / TLSAssets.CA_CERT
    tmp_truststore_path = Path('/tmp') / TLSAssets.JKS_TRUSTSTORE
    _ = java.exec_run(f'rm {tmp_truststore_path}')  # delete truststore if it exists (needed for local SCT runs)
    exit_code, output = java.exec_run(
        f'keytool -importcert -noprompt -file {tmp_cert_path} -keystore {tmp_truststore_path} '
        f'-storepass {jks_password}')
    if exit_code != 0:
        raise DockerException(f"Error importing CA certificate to Java truststore: {output.decode('utf-8')}")

    # Copy and extract truststore tar archive from container
    tar_stream, _ = java.get_archive(tmp_truststore_path)

    file_obj = io.BytesIO()
    for chunk in tar_stream:
        file_obj.write(chunk)
    file_obj.seek(0)
    with tarfile.open(fileobj=file_obj) as tar:
        member = tar.getmember(tmp_truststore_path.name)
        with tar.extractfile(member) as truststore_file:
            with open(jks_file, 'wb') as output_file:
                output_file.write(truststore_file.read())


def export_pem_cert_to_pkcs12_keystore(
        cert_file: Path, cert_key_file: Path, dst_pkcs12_file: Path,
        cert_key_password: Any = None, p12_password: str = 'cassandra') -> None:
    """Export a PEM formatted certificate to a PKCS12 keystore."""
    with open(cert_file, 'rb') as file:
        cert = x509.load_pem_x509_certificate(file.read())
    with open(cert_key_file, 'rb') as file:
        if password := cert_key_password:
            password = cert_key_password.encode()
        key = serialization.load_pem_private_key(file.read(), password=password)

    pkcs12 = serialize_key_and_certificates(
        name=b'scylladb', key=key, cert=cert, cas=None,
        encryption_algorithm=serialization.BestAvailableEncryption(p12_password.encode()))
    with open(dst_pkcs12_file, 'wb') as file:
        file.write(pkcs12)


def cleanup_ssl_config():
    """Remove SCT ssl_config artifacts that are dynamically created during the test"""
    ssl_conf_dir = Path(get_data_dir_path('ssl_conf'))
    for item in ssl_conf_dir.iterdir():
        if item.is_dir() and item.name not in {'client', 'example'}:
            shutil.rmtree(item)
        elif item.is_file():
            item.unlink()


def update_certificate(node: BaseNode) -> None:
    db_csr_file = node.ssl_conf_dir / TLSAssets.DB_CSR
    db_crt_file = node.ssl_conf_dir / TLSAssets.DB_CERT
    ca_file: Path = CA_CERT_FILE
    ca_key_file: Path = CA_KEY_FILE

    """Update server certificate."""
    # Load CSR, CA certificate and key
    with open(db_csr_file, 'rb') as file:
        csr = x509.load_pem_x509_csr(file.read())
    with open(ca_file, 'rb') as file:
        ca_cert = x509.load_pem_x509_certificate(file.read())
    with open(ca_key_file, 'rb') as file:
        ca_key = serialization.load_pem_private_key(file.read(), password=CA_DEFAULT_PASSWORD.encode())

    san_extension = csr.extensions.get_extension_for_oid(x509.ExtensionOID.SUBJECT_ALTERNATIVE_NAME).value

    # Create new certificate
    new_cert = (
        x509.CertificateBuilder()
        .subject_name(csr.subject)
        .issuer_name(ca_cert.subject)
        .public_key(csr.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.utcnow())
        .not_valid_after(datetime.utcnow() + timedelta(days=180))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(csr.public_key()), critical=False)
        .add_extension(san_extension, critical=False)  # Add SAN extension from CSR
        .sign(private_key=ca_key, algorithm=hashes.SHA256())
    )

    # Write new certificate to file
    with open(db_crt_file, 'wb') as crt_file:
        crt_file.write(new_cert.public_bytes(serialization.Encoding.PEM))


def c_s_transport_str(peer_verification: bool, client_mtls: bool) -> str:
    """Build transport string for cassandra-stress."""
    transport_str = f'truststore={SCYLLA_SSL_CONF_DIR}/{TLSAssets.JKS_TRUSTSTORE} truststore-password=cassandra'
    if peer_verification:
        transport_str += ' hostname-verification=true'
    if client_mtls:
        transport_str += f' keystore={SCYLLA_SSL_CONF_DIR}/{TLSAssets.PKCS12_KEYSTORE} keystore-password=cassandra'
    return transport_str


def cql_stress_transport_str(peer_verification: bool) -> str:
    """Build transport string for cassandra-stress."""
    transport_str = f'truststore={SCYLLA_SSL_CONF_DIR}/{TLSAssets.CA_CERT} truststore-password=cassandra'
    if peer_verification:
        transport_str += ' hostname-verification=true'
    return transport_str


def create_ca(localhost):
    _create_ca()
    _import_ca_to_jks_truststore(localhost)
