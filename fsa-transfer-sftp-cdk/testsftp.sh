#! /bin/sh

aws transfer test-identity-provider \
  --server-id s-2d7c1246db474ba6a \
  --user-name S_DART-CERT \
  --user-password MyDartTest
