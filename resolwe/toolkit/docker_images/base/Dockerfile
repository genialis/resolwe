FROM docker.io/fedora:25

LABEL MAINTAINER Resolwe authors https://github.com/genialis/resolwe

# XXX: Remove this after Resolwe learns how to mount re-* tools into every container
COPY re-import.sh /usr/local/bin/re-import
# XXX: Remove this after Resolwe learns how to mount re-* tools into every container
COPY curlprogress.py /usr/local/bin/


RUN pip3 install resolwe-runtime-utils && \
    # XXX: Remove this hack after obsoleting re-checkrc
    echo 're-checkrc() { _re-checkrc $? "$@"; }' >> ~/.bash_profile && \
    # XXX: Remove this after Resolwe learns how to mount re-* tools into every container
    chmod +x /usr/local/bin/re-import && \
    chmod +x /usr/local/bin/curlprogress.py
