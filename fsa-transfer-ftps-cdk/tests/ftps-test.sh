#! /bin/sh

curl -v --ftp-ssl --ftp-pasv \
  --user 'S_DART-CERT:MyDartTest' \
  ftp://ftps.steamfpac.com/
