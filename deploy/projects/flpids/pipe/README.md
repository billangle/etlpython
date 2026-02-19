## Project FLPIDS

[Jenkins PROD DEPLOY Pipeline] (https://jenkinsfsa.dl.usda.gov/job/BuildProcess/job/DeployFlpids/)

[Jenkins DEV DEPLOY Pipeline] (https://jenkinsfsa-dev.dl.usda.gov/job/BuildProcess/job/DeployFlpids/)

## FLPIDS PROD Data Pipeline Jobs

[FLPIDS LOAD file_pattern\": \"^(\\\\w+)\$\", \"echo_folder\": \"gls\" ] (https://jenkinsfsa.dl.usda.gov/job/FLPIDS-FileChecker/job/FSA-PROD-FLPIDS-LOAD-FileChecker/)


[FLPIDS SCIMS "file_pattern\": \"(MOYWRSC_[A-Z]{4})\\\\.txt\$\",\"echo_folder\": \"plas\", ] (https://jenkinsfsa.dl.usda.gov/job/FLPIDS-FileChecker/job/FSA-PROD-FLPIDS-SCIMS-FileChecker/
)

[FLPIDS WEEKLY '{\"file_pattern\": \"^(wk.moyr540)\\\\.data\$\", \"echo_folder\": \"plas\" ]  (https://jenkinsfsa.dl.usda.gov/job/FLPIDS-FileChecker/job/FSA-PROD-FLPIDS-Weekly-FileChecker/)


[FLPIDS MONTHLY '{\"file_pattern\": \"^(mo.moyr540)\\\\.data\$\", \"echo_folder\": \"plas\"] (https://jenkinsfsa.dl.usda.gov/job/FLPIDS-FileChecker/job/FSA-PROD-FLPIDS-Monthly-FileChecker/)


[FLPIDS NATS '{\"file_pattern\": \"^(FILE_NATS_\\\\w+)\\\\.(\\\\d{8})\\\\.csv\$\", \"echo_folder\": \"nats\" ] (https://jenkinsfsa.dl.usda.gov/job/FLPIDS-FileChecker/job/FSA-PROD-FLPIDS-NATS-FileChecker/)


[FLPIDS RPT \"file_pattern\": \"(MFO900\\\\.MOYRPT\\\\.\\\\w{1,128})\\\\.DATA\\\\.(\\\\d{14})\$\", \"echo_folder\": \"plas\"] (https://jenkinsfsa.dl.usda.gov/job/FLPIDS-FileChecker/job/FSA-PROD-FLPIDS-Report-FileChecker/)


[FLPIDS -CAORPT-CONGDIST '{\"file_pattern\": \"(\\\\w{1,128})\\\\.(\\\\d{8})\\\\.csv\$\",\"echo_folder\": \"mrtxdb\",\"echo_subfolder\": \"weekly\" ] (https://jenkinsfsa.dl.usda.gov/job/FLPIDS-FileChecker/job/FSA-PROD-FLPIDS-CAORPT-CONGDIST-FileChecker/)


[FLPIDS CAORPT '{\"file_pattern\": \"(\\\\w{1,128})\\\\.(\\\\d{8})\\\\.csv\$\",\"echo_folder\": \"mrtxdb\",\"echo_subfolder\": \"daily\" ] (https://jenkinsfsa.dl.usda.gov/job/FLPIDS-FileChecker/job/FSA-PROD-FLPIDS-CAORPT-FileChecker/)


[FLPIDS CAORPT-ORGANIZATION {\"file_pattern\": \"(ORGN|DVSN).TXT\",\"echo_folder\": \"plas\" ] (https://jenkinsfsa.dl.usda.gov/job/FLPIDS-FileChecker/job/FSA-PROD-FLPIDS-CAORPT-ORGANIZATION-FileChecker/)


[FLPIDS OBLRPT '{\"file_pattern\": \"^(FGOBGL)\\\\.TXT\$\", \"echo_folder\": \"plas\"  ] (https://jenkinsfsa.dl.usda.gov/job/FLPIDS-FileChecker/job/FSA-PROD-FLPIDS-OBLRPT-FileChecker/)


[FLPIDS OFCRPT file_pattern\": \"(officeinfo)\\\\.txt\",\"echo_folder\": \"plas\"] (https://jenkinsfsa.dl.usda.gov/job/FLPIDS-FileChecker/job/FSA-PROD-FLPIDS-OFCRPT-FileChecker/)