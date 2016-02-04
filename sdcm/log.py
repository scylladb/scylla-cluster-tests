import logging


class SDCMAdapter(logging.LoggerAdapter):

    """
    Logging adapter to prepend class string identifier to message.
    """

    def process(self, msg, kwargs):
        return '%s: %s' % (self.extra['prefix'], msg), kwargs
