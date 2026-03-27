export SONAR_HOST_URL="https://sonarqube.example.com"
export SONAR_TOKEN="your_token_here"

python3 sonarqube_pdf_report.py \
  --project-key your_project_key \
  --branch main \
  --output sonar-report.pdf