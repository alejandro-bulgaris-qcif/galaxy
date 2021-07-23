#!/usr/bin/env python
"""
Download a file from a url using token and token_key pair in the request headers
"""

import json
import math
import optparse
import os
import tarfile
import shutil
from base64 import b64decode

from galaxy.datatypes import sniff


# Set max size of archive/file that will be handled to be 100 GB. This is
# arbitrary and should be adjusted as needed.
MAX_SIZE = 100 * math.pow(2, 30)


def get_file_sources(file_sources_path):
    assert os.path.exists(file_sources_path), "file sources path [%s] does not exist" % file_sources_path
    from galaxy.files import ConfiguredFileSources
    with open(file_sources_path) as f:
        file_sources_as_dict = json.load(f)
    file_sources = ConfiguredFileSources.from_dict(file_sources_as_dict)
    return file_sources

def is_gz_file(filepath):
    with open(filepath, 'rb') as test_f:
        return test_f.read(2) == b'\x1f\x8b'

def main(options, args):
    is_url = bool(options.is_url)
    url_source, dest_dir, token_name, token_key = args

    if options.is_b64encoded:
        url_source = b64decode(url_source).decode('utf-8')
        dest_dir = b64decode(dest_dir).decode('utf-8')
        token_name = b64decode(token_name).decode('utf-8')
        token_key = b64decode(token_key).decode('utf-8')
    
    #TODO user agent has to be passed in from or defined by the system that initiates the transfer
    user_agent = "data.bioplatforms.com - galaxy/api/histories - bioblend/0.13.0"

    # Get file from URL.
    if is_url:
        if url_source and dest_dir and token_name and token_key:
            downloaded_file_tmp_name = sniff.stream_url_to_file_with_token(url_source, token_name, token_key, user_agent)
            if downloaded_file_tmp_name and not is_gz_file(downloaded_file_tmp_name) and not tarfile.is_tarfile(downloaded_file_tmp_name):
                file_name = os.path.basename(url_source)
                copy_file_path = dest_dir + "/datasets"
                if not os.path.exists(copy_file_path):
                    os.makedirs(copy_file_path)
                copy_file_path = dest_dir + "/datasets/"+str(file_name)
                shutil.copyfile(downloaded_file_tmp_name, copy_file_path)

if __name__ == "__main__":
    # Parse command line.
    parser = optparse.OptionParser()
    parser.add_option('-U', '--url', dest='is_url', action="store_true", help='Source is a URL.')
    parser.add_option('-F', '--file', dest='is_file', action="store_true", help='Source is a file.')
    parser.add_option('-e', '--encoded', dest='is_b64encoded', action="store_true", default=False, help='Source and destination dir values are base64 encoded.')
    parser.add_option('--file-sources', type=str, help='file sources json')
    (options, args) = parser.parse_args()
    main(options, args)
