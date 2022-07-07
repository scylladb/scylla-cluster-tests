def recover_config_files(node):
    """Since Scylla 5.0 some config files on rhel get backed up upon Scylla uninstallation.
    Default ones are used after installation.

    Let's recover them so Scylla can start without requiring all the setup."""
    if node.is_rhel_like():
        node.remoter.run('sudo cp /etc/scylla.d/io.conf.rpmsave /etc/scylla.d/io.conf', ignore_status=True)
        node.remoter.run('sudo cp /etc/scylla.d/memory.conf.rpmsave /etc/scylla.d/memory.conf', ignore_status=True)
        node.remoter.run('sudo cp /etc/scylla.d/cpuset.conf.rpmsave /etc/scylla.d/cpuset.conf', ignore_status=True)
        node.remoter.run('sudo cp /etc/scylla/scylla.yaml.rpmsave /etc/scylla/scylla.yaml', ignore_status=True)
