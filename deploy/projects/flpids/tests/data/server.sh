#! /bin/sh

curl -v --ftp-ssl --ftp-ssl-control \
--user "S_DART-CERT:MyDartTest" \
  ftp://ftps.steamfpac.com/ 