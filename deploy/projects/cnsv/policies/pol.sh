#! /bin/sh



aws iam put-role-policy \
  --role-name FSA-DEV-FPACCARS-SfnNestedCaller \
  --policy-name NestedStateMachineAccess \
  --policy-document file://permissions-policy.json
