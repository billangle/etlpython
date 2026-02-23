#! /bin/sh

# Run all failure-path tests.
# Each script invokes DynaCheckFile with LAMBDA_ARN pointing to TestFailureJob
# so the state machine routes through FinalizeJobOnCatch (ERROR path).

./test_flpids_nats.sh
./test_flpids_scims.sh
./test_flpidsload.sh
./test_flpidsrpt.sh
./test_rc540_monthly.sh
./test_rc540_weekly.sh
./test_flpids_caorpt.sh
./test_flpids_caorpt_congdist.sh
./test_flpids_caorpt_organization.sh
./test_flpids_oblrpt.sh
./test_flpids_ofcrpt.sh
