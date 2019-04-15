def setup_stdout_logger():
    import sys
    import logging
    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging.INFO)
    rootLogger.addHandler(logging.StreamHandler(sys.stdout))
    return rootLogger
