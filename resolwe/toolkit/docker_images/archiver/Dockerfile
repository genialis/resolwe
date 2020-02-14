FROM docker.io/resolwe/base:fedora-31

RUN rpmkeys --import file:///etc/pki/rpm-gpg/RPM-GPG-KEY-fedora-31-x86_64 && \
    dnf install -y --setopt=tsflags=nodocs zip && \
    dnf clean all
