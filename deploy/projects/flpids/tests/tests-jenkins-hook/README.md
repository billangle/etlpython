# FTPS Lambda Test Scripts (Env Port + mktemp + quoted heredoc)

## Required environment variables
- FUNCTION   : Lambda function name
- FTPS_PORT  : FTPS port (21 or 990)

## Why quoted heredoc?
We use `<<'EOF'` so that backslashes in regex patterns are not collapsed by the shell.
Then we substitute the `ftps_port` placeholder via `sed`.

## Usage
```bash
export FUNCTION=FSA-dev-FpacFLPIDSCHK-FileChecker2
export FTPS_PORT=21   # or 990
./01_gls_generic_ods.sh
```
