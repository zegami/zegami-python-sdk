# -*- coding: utf-8 -*-
# Copyright 2021 Zegami Ltd

"""helper code."""

import sys


def guess_data_mimetype(data):

    fallback_mimetype = 'application/octet-stream'

    try:
        import magic
        LIBMAGIC_ENABLED = True
    except ImportError:
        sys.stdout.write(
            'WARNING: the libmagic library for checking mimetypes is not installed.\n'
            'If you need this functionality please install this locally:\n'
            'For MacOS: `brew install libmagic`\n'
            'For Windows check python-magic-bin is installed as a python library\n'
        )
        LIBMAGIC_ENABLED = False

    if LIBMAGIC_ENABLED:
        try:
            return magic.from_buffer(data, mime=True)
        except TypeError:
            pass
    return fallback_mimetype
