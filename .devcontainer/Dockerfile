FROM centos/devtoolset-7-toolchain-centos7

USER 0
RUN rpm -Uvh https://yum.postgresql.org/11/redhat/rhel-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm
RUN yum -y install  https://centos7.iuscommunity.org/ius-release.rpm
RUN yum update -y
RUN yum install -y redhat-lsb
RUN yum install -y git2u-all
RUN yum install -y postgresql11-devel
RUN yum install -y unixODBC-devel
RUN yum install -y centos-release-scl
RUN yum install -y epel-release
RUN yum install -y llvm-toolset-7
RUN yum install -y llvm5.0
ENV PATH $PATH:/usr/pgsql-11/bin
ENV SHELL /bin/bash
