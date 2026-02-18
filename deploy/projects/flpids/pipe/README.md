## Project FLPIDS

[Jenkins PROD DEPLOY Pipeline] (https://jenkinsfsa.dl.usda.gov/job/BuildProcess/job/DeployFlpids/)

[Jenkins DEV DEPLOY Pipeline] (https://jenkinsfsa-dev.dl.usda.gov/job/BuildProcess/job/DeployFlpids/)

## FLPIDS PROD Data Pipeline Jobs

[FLPIDS LOAD file_pattern\": \"^(\\\\w+)\$\", \"echo_folder\": \"gls\" ] (https://jenkinsfsa.dl.usda.gov/job/FSA-PROD-FLPIDS-LOAD-FileChecker/)

[FLPIDS SCIMS "file_pattern\": \"(MOYWRSC_[A-Z]{4})\\\\.txt\$\",\"echo_folder\": \"plas\", ] (https://jenkinsfsa.dl.usda.gov/job/FSA-PROD-FLPIDS-SCIMS-FileChecker/)

[FLPIDS WEEKLY '{\"file_pattern\": \"^(wk.moyr540)\\\\.data\$\", \"echo_folder\": \"plas\" ] (https://jenkinsfsa.dl.usda.gov/job/FSA-PROD-FLPIDS-Weekly-FileChecker/)

[FLPIDS MONTHLY '{\"file_pattern\": \"^(mo.moyr540)\\\\.data\$\", \"echo_folder\": \"plas\"] (https://jenkinsfsa.dl.usda.gov/job/FSA-PROD-FLPIDS-Monthly-FileChecker/)

[FLPIDS NATS '{\"file_pattern\": \"^(FILE_NATS_\\\\w+)\\\\.(\\\\d{8})\\\\.csv\$\", \"echo_folder\": \"nats\" ] (https://jenkinsfsa.dl.usda.gov/job/FSA-PROD-FLPIDS-NATS-FileChecker/)

[FLPIDS RPT \"file_pattern\": \"(MFO900\\\\.MOYRPT\\\\.\\\\w{1,128})\\\\.DATA\\\\.(\\\\d{14})\$\", \"echo_folder\": \"plas\"] (https://jenkinsfsa.dl.usda.gov/job/FSA-PROD-FLPIDS-Report-FileChecker/)

