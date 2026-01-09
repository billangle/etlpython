# FTPS Lambda Test Scripts

These scripts invoke the **FTPS file check Lambda** with different `file_pattern` + `echo_folder` combinations.

## Usage

1. Ensure AWS CLI is configured for the target account/role.
2. Run any script:
   ```bash
   ./01_gls_generic_ods.sh
   ```
3. Each script writes `response.json` in the current directory.

## Notes

- Scripts use `--cli-binary-format raw-in-base64-out` for AWS CLI v2 compatibility.
- Payload values are embedded as JSON inside single quotes. Regex backslashes are correctly escaped for JSON.
