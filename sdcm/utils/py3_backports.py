import sys
import time
import math
from datetime import timedelta, datetime

'''TODO: backports from python 3.3'''


def _fromtimestamp(t, tz=None):
    """Construct a datetime from a POSIX timestamp (like time.time()).
    A timezone info object may be passed in as well.
    """
    frac, t = math.modf(t)
    us = int(round(frac * 1e6))
    if us >= 1000000:
        t += 1
        us -= 1000000
    elif us < 0:
        t -= 1
        us += 1000000

    converter = time.gmtime
    y, m, d, hh, mm, ss, weekday, jday, dst = converter(t)
    ss = min(ss, 59)    # clamp out leap seconds if the platform has them
    result = datetime(y, m, d, hh, mm, ss, us, tz)
    if tz is None:
        # As of version 2015f max fold in IANA database is
        # 23 hours at 1969-09-30 13:00:00 in Kwajalein.
        # Let's probe 24 hours in the past to detect a transition:
        max_fold_seconds = 24 * 3600

        # On Windows localtime_s throws an OSError for negative values,
        # thus we can't perform fold detection for values of time less
        # than the max time fold. See comments in _datetimemodule's
        # version of this method for more details.
        if t < max_fold_seconds and sys.platform.startswith("win"):
            return result

        y, m, d, hh, mm, ss = converter(t - max_fold_seconds)[:6]
        probe1 = datetime(y, m, d, hh, mm, ss, us, tz)
        trans = result - probe1 - timedelta(0, max_fold_seconds)
        if trans.days < 0:
            y, m, d, hh, mm, ss = converter(t + trans // timedelta(0, 1))[:6]
            probe2 = datetime(y, m, d, hh, mm, ss, us, tz)
            if probe2 == result:
                result._fold = 1
    else:
        result = tz.fromutc(result)
    return result
